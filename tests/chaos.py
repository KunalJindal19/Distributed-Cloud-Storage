#!/usr/bin/env python3
"""
Chaos test for the object store (block 9, AC-9.2).

Runs the Docker Compose stack with fault injection on every internal call (delays, lost requests,
lost responses, duplicates), drives it with four concurrent clients for CHAOS_MINUTES, and keeps
breaking things: stopping, pausing and partitioning one worker at a time, restarting, killing and
pausing the controller, and corrupting chunk files. Then it stops the disruption and checks that
the cluster converges and that nothing acknowledged was lost:

  1. status: safe mode off, nothing under/over-replicated, missing or pending, no queued commands,
     no corrupt copies;
  2. fsck HEALTHY and quiescent, no orphans on disk, no difference between disks and the block map;
  3. every acknowledged, not deleted object downloads with the right SHA-256;
  4. every deleted object answers 404 and none of its chunks is on any worker's disk;
  5. invariant_violations == 0;
  6. the event-driven core is clean: index_mismatches, writer_task_failures and namespace_reconciled
     are 0, the apply queue is empty, no op is pending (DELETEs included), no resync is open.

It runs with RESYNC_INTERVAL = 3600 (the default): there is no frequent resync to fall
back on, so deletions and orphans must converge through the versioned ops and the one-time resync
request when orphan GC becomes allowed.

Standard library only. Usage:
    python3 tests/chaos.py [--minutes 5] [--keep-stack] [--no-fresh]
Environment: CHAOS_MINUTES, BASE_URL (default http://localhost:$CONTROLLER_PORT, i.e. 9000).
"""

import argparse
import asyncio
import base64
import hashlib
import http.client as httplib
import json
import os
import random
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if not os.path.exists(os.path.join(ROOT, "docker-compose.yml")) and os.path.exists("docker-compose.yml"):
    ROOT = os.getcwd()  # run from a copy outside the repository: use the current directory
BASE = os.environ.get("BASE_URL", "http://localhost:" + os.environ.get("CONTROLLER_PORT", "9000"))
API = BASE + "/api/v1"
# Every request is made as the admin account (README "Accounts").
AUTH = "Basic " + base64.b64encode("{}:{}".format(
    os.environ.get("ADMIN_EMAIL", "admin@gmail.com"), os.environ.get("ADMIN_PASSWORD", "admin123")).encode()).decode()
WORKERS = ["worker1", "worker2", "worker3", "worker4"]
NETWORK = os.environ.get("COMPOSE_PROJECT_NAME", "objectstore") + "_objectstore-net"  # follows the Compose project
COMPOSE = ["docker", "compose", "-f", "docker-compose.yml", "-f", "docker-compose.chaos.yml"]
RETRY_FOR = 180  # seconds a client keeps retrying one operation

lock = threading.Lock()
acked = {}        # object_id -> sha256 of the content (acknowledged with 200/201)
deleted = set()   # object ids whose DELETE was acknowledged (204, or 404 after a retry)
busy = set()      # object ids with a delete in progress (not used for downloads)
stats = {"uploads": 0, "replays": 0, "downloads": 0, "deletes": 0, "patches": 0, "retries": 0,
         "errors": 0, "disruptions": 0}
problems = []     # anything that indicates lost or wrong data


def log(message):
    print(time.strftime("%H:%M:%S"), message, flush=True)


def problem(message):
    with lock:
        problems.append(message)
        stats["errors"] += 1
    log("PROBLEM " + message)


# ---- HTTP ------------------------------------------------------------------------------------

def http(method, url, body=None, headers=None, timeout=60):
    """Returns (status, headers, body bytes). Raises OSError on any transport failure, including a
    body cut off mid-stream (http.client reports that as IncompleteRead, not as an OSError)."""
    request = urllib.request.Request(url, data=body, method=method, headers={"Authorization": AUTH, **(headers or {})})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, dict(response.headers), response.read()
    except urllib.error.HTTPError as error:
        try:
            return error.code, dict(error.headers), error.read()
        except httplib.HTTPException as broken:
            raise OSError("broken error response: %r" % broken) from broken
    except httplib.HTTPException as broken:
        raise OSError("transfer broke off: %r" % broken) from broken


def multipart(name, data, boundary):
    head = ("--%s\r\nContent-Disposition: form-data; name=\"file\"; filename=\"%s\"\r\n"
            "Content-Type: application/octet-stream\r\n\r\n" % (boundary, name)).encode()
    return head + data + ("\r\n--%s--\r\n" % boundary).encode()


def with_retries(what, attempt):
    """Calls attempt() until it returns a definitive (status, headers, body); retries transport
    errors and 503 answers with back-off for up to RETRY_FOR seconds."""
    deadline = time.monotonic() + RETRY_FOR
    pause = 0.5
    retried = False
    while True:
        try:
            status, headers, body = attempt()
            if status != 503:
                return status, headers, body, retried
        except OSError:
            pass
        if time.monotonic() > deadline:
            raise RuntimeError(what + " still failing after %d s" % RETRY_FOR)
        retried = True
        with lock:
            stats["retries"] += 1
        time.sleep(pause)
        pause = min(pause * 2, 5)


def status():
    return json.loads(http("GET", API + "/cluster/status", timeout=10)[2])


# ---- client operations -------------------------------------------------------------------------

def op_upload(rng):
    size = rng.choice([0, 1, 1000, 300_000, 1_048_576, 2_097_152 + 17, 3_500_000, rng.randint(0, 5_242_880)])
    data = os.urandom(size)
    sha = hashlib.sha256(data).hexdigest()
    key = "chaos-" + str(uuid.uuid4())
    boundary = "----chaos" + uuid.uuid4().hex
    body = multipart("chaos-%d.bin" % size, data, boundary)
    headers = {"Content-Type": "multipart/form-data; boundary=" + boundary, "Idempotency-Key": key}
    code, response_headers, answer, _ = with_retries(
        "upload", lambda: http("POST", API + "/objects", body, headers, timeout=120))
    if code in (200, 201):
        result = json.loads(answer)
        if result["checksum"] != sha or result["size"] != size:
            problem("upload %s acknowledged with checksum/size %s/%s, expected %s/%s"
                    % (result["object_id"], result["checksum"], result["size"], sha, size))
            return
        with lock:
            acked[result["object_id"]] = sha
            stats["uploads"] += 1
            if code == 200:
                stats["replays"] += 1
    else:
        problem("upload answered %d: %s" % (code, answer[:200]))


def pick(rng):
    with lock:
        candidates = [o for o in acked if o not in deleted and o not in busy]
        if not candidates:
            return None
        return rng.choice(candidates)


def op_download(rng):
    object_id = pick(rng)
    if object_id is None:
        return
    deadline = time.monotonic() + RETRY_FOR
    while True:
        try:
            code, headers, body = http("GET", API + "/objects/" + object_id, timeout=120)
        except OSError:
            code, body = None, b""  # includes a body cut off mid-stream (IncompleteRead)
        if code == 200:
            expected = acked[object_id]
            if hashlib.sha256(body).hexdigest() == expected:
                with lock:
                    stats["downloads"] += 1
                return
            if len(body) == int(headers.get("Content-Length", -1)):
                problem("download of %s returned wrong bytes" % object_id)
                return
        elif code == 404:
            with lock:
                gone = object_id in deleted or object_id in busy
            if not gone:
                problem("acknowledged object %s answered 404" % object_id)
            return
        elif code not in (None, 503):
            problem("download of %s answered %s" % (object_id, code))
            return
        if time.monotonic() > deadline:
            problem("download of %s still failing after %d s" % (object_id, RETRY_FOR))
            return
        with lock:
            stats["retries"] += 1
        time.sleep(1)


def op_delete(rng):
    object_id = pick(rng)
    if object_id is None:
        return
    with lock:
        busy.add(object_id)
    code, _, answer, retried = with_retries("delete", lambda: http("DELETE", API + "/objects/" + object_id))
    if code == 204 or (code == 404 and retried):
        with lock:
            deleted.add(object_id)
            stats["deletes"] += 1
    else:
        problem("delete of %s answered %d: %s" % (object_id, code, answer[:200]))
    with lock:
        busy.discard(object_id)


def op_patch(rng):
    object_id = pick(rng)
    if object_id is None:
        return
    body = json.dumps({"chaos": str(rng.randint(0, 1_000_000))}).encode()
    code, _, answer, _ = with_retries("patch", lambda: http(
        "PATCH", API + "/objects/" + object_id + "/metadata", body, {"Content-Type": "application/json"}))
    if code == 200:
        with lock:
            stats["patches"] += 1
    elif code == 404:
        with lock:
            gone = object_id in deleted or object_id in busy
        if not gone:
            problem("patch of acknowledged object %s answered 404" % object_id)
    else:
        problem("patch of %s answered %d: %s" % (object_id, code, answer[:200]))


async def client(index, stop):
    rng = random.Random(index * 7919 + int(time.time()))
    operations = [op_upload] * 4 + [op_download] * 4 + [op_delete] + [op_patch] * 2
    while not stop.is_set():
        operation = rng.choice(operations)
        try:
            await asyncio.to_thread(operation, rng)
        except Exception as error:  # noqa: BLE001 - a broken operation is a finding, not a crash
            problem("client %d %s: %s" % (index, operation.__name__, error))


# ---- disruption ----------------------------------------------------------------------------------

def docker(*args, check=False):
    return subprocess.run(["docker", *args], capture_output=True, text=True, check=check)


def corrupt_random_chunk(rng):
    worker = rng.choice(WORKERS)
    listing = docker("exec", worker, "sh", "-c", "ls /data/chunks | grep -v '\\.meta$' | grep -v '\\.tmp$' || true").stdout.split()
    if not listing:
        return "no chunk on %s to corrupt" % worker
    chunk = rng.choice(listing)
    docker("exec", worker, "sh", "-c",
           "printf 'XXXX' | dd of=/data/chunks/%s bs=1 seek=0 conv=notrunc status=none" % chunk)
    return "corrupted %s on %s" % (chunk, worker)


def disrupt_once(rng):
    action = rng.choice(["stop", "pause", "partition", "restart_controller", "kill_controller",
                         "pause_controller", "corrupt"])
    worker = rng.choice(WORKERS)
    if action == "stop":
        docker("stop", worker)
        log("disruption: docker stop %s (25 s)" % worker)
        time.sleep(25)
        docker("start", worker)
    elif action == "pause":
        docker("pause", worker)
        log("disruption: docker pause %s (8 s)" % worker)
        time.sleep(8)
        docker("unpause", worker)
    elif action == "partition":
        docker("network", "disconnect", NETWORK, worker)
        log("disruption: network disconnect %s (20 s)" % worker)
        time.sleep(20)
        docker("network", "connect", NETWORK, worker)
    elif action == "restart_controller":
        log("disruption: docker restart controller")
        docker("restart", "controller")
    elif action == "kill_controller":
        log("disruption: docker kill controller, then start it")
        docker("kill", "controller")
        time.sleep(2)
        docker("start", "controller")
    elif action == "pause_controller":
        log("disruption: docker pause controller (8 s)")
        docker("pause", "controller")
        time.sleep(8)
        docker("unpause", "controller")
    else:
        log("disruption: " + corrupt_random_chunk(rng))
    with lock:
        stats["disruptions"] += 1


async def disruptor(stop):
    rng = random.Random(int(time.time()))
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=rng.uniform(15, 40))
            return
        except asyncio.TimeoutError:
            pass
        await asyncio.to_thread(disrupt_once, rng)


def restore_everything():
    for container in ["controller"] + WORKERS:
        state = docker("inspect", "-f", "{{.State.Paused}} {{.State.Running}}", container).stdout.split()
        if state and state[0] == "true":
            docker("unpause", container)
        if state and state[1] == "false":
            docker("start", container)
    for worker in WORKERS:
        networks = docker("inspect", "-f", "{{json .NetworkSettings.Networks}}", worker).stdout
        if NETWORK not in networks:
            docker("network", "connect", NETWORK, worker)


# ---- convergence ---------------------------------------------------------------------------------

def wait_until(what, predicate, timeout):
    started = time.monotonic()
    last = None
    while time.monotonic() - started < timeout:
        try:
            ok, last = predicate()
            if ok:
                log("%s after %.0f s" % (what, time.monotonic() - started))
                return True
        except (OSError, ValueError, KeyError) as error:
            last = str(error)
        time.sleep(2)
    log("TIMED OUT waiting for %s; last: %s" % (what, last))
    return False


def converged():
    s = status()
    queued = sum(w["queued_commands"] for w in s["workers"])
    alive = sum(1 for w in s["workers"] if w["state"] == "ALIVE" and w["has_reported"])
    ok = (not s["safe_mode"] and alive == 4 and s["under_replicated"] == 0 and s["over_replicated"] == 0
          and s["missing_chunks"] == 0 and s["pending_replications"] == 0 and queued == 0
          and s["replicas"]["corrupt"] == 0 and s["replicas"]["excess"] == 0)
    summary = {k: s[k] for k in ("safe_mode", "under_replicated", "over_replicated", "missing_chunks",
                                 "pending_replications")}
    summary.update(queued=queued, alive=alive, replicas=s["replicas"])
    return ok, summary


def fsck_clean():
    result = json.loads(http("GET", API + "/fsck", timeout=120)[2])
    d = result["disk_vs_map"]
    ok = (result["status"] == "HEALTHY" and result["quiescent"] and result["orphans_on_disk"] == 0
          and not d["on_disk_not_in_map"] and not d["in_map_not_on_disk"] and not d["checksum_mismatch"]
          and not result["invariant_violations"])
    return ok, result


def chunks_on_disks():
    files = set()
    for worker in WORKERS:
        listing = docker("exec", worker, "sh", "-c", "ls /data/chunks").stdout.split()
        files.update(name for name in listing if not name.endswith(".meta") and not name.endswith(".tmp"))
    return files


def verify(results):
    """The final checks; returns a list of (check, passed, detail)."""
    checks = []
    ok = wait_until("convergence", converged, 180)
    checks.append(("status converged (no under/over/missing/pending/queued/corrupt)", ok, converged()[1]))

    # Orphans are only collected ORPHAN_GC_DELAY (60 s) after a controller start, from the full
    # report every worker is asked for once GC becomes allowed: allow for both.
    ok = wait_until("fsck HEALTHY, quiescent, no orphans, disks match the map", fsck_clean, 240)
    _, result = fsck_clean()
    checks.append(("fsck HEALTHY, quiescent, orphans 0, disk_vs_map empty", ok,
                   {k: result[k] for k in ("status", "quiescent", "orphans_on_disk", "disk_vs_map", "unreachable_workers")}))

    with lock:
        alive_objects = {o: sha for o, sha in acked.items() if o not in deleted}
        gone = set(deleted)
    started = time.monotonic()
    wrong = []
    for object_id, sha in alive_objects.items():
        code, _, body, _ = with_retries("verify download", lambda: http("GET", API + "/objects/" + object_id, timeout=120))
        if code != 200 or hashlib.sha256(body).hexdigest() != sha:
            wrong.append((object_id, code))
    checks.append(("every acknowledged object downloads with the right SHA-256 (%d objects)" % len(alive_objects),
                   not wrong, wrong[:5]))
    log("verified %d downloads in %.0f s" % (len(alive_objects), time.monotonic() - started))
    started = time.monotonic()

    still_there = []
    on_disk = chunks_on_disks()
    for object_id in gone:
        code = with_retries("verify delete", lambda: http("GET", API + "/objects/" + object_id + "/metadata"))[0]
        leftovers = [c for c in on_disk if c.startswith(object_id)]
        if code != 404 or leftovers:
            still_there.append((object_id, code, len(leftovers)))
    checks.append(("every deleted object is 404 with no chunk on any disk (%d objects)" % len(gone),
                   not still_there, still_there[:5]))
    log("verified %d deletions in %.0f s" % (len(gone), time.monotonic() - started))

    final = status()
    counters = final["counters"]
    violations = counters.get("invariant_violations", 0)
    checks.append(("invariant_violations == 0", violations == 0, violations))
    core = {name: counters.get(name, 0) for name in ("index_mismatches", "writer_task_failures", "namespace_reconciled")}
    checks.append(("index_mismatches, writer_task_failures, namespace_reconciled == 0", not any(core.values()), core))
    ops = final.get("ops", {}).get("by_state", {})
    idle = {"apply_queue_depth": final.get("apply_queue_depth"), "ops": ops, "resyncing": final.get("resyncing"),
            "report_sessions_open": final.get("report_sessions_open")}
    checks.append(("apply queue empty, no pending op, no resync open",
                   final.get("apply_queue_depth") == 0 and sum(ops.values()) == 0 and final.get("resyncing") == 0
                   and final.get("report_sessions_open") == 0, idle))
    checks.append(("no data problems observed by the clients", not problems, problems[:5]))
    return checks


# ---- main ----------------------------------------------------------------------------------------

async def run(minutes):
    stop = asyncio.Event()
    tasks = [asyncio.create_task(client(i, stop)) for i in range(4)]
    tasks.append(asyncio.create_task(disruptor(stop)))
    deadline = time.monotonic() + minutes * 60
    while time.monotonic() < deadline:
        await asyncio.sleep(15)
        with lock:
            log("progress: " + ", ".join("%s=%d" % kv for kv in stats.items())
                + ", live objects=%d" % len([o for o in acked if o not in deleted]))
    log("stopping load and disruption")
    stop.set()
    await asyncio.gather(*tasks)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--minutes", type=float, default=float(os.environ.get("CHAOS_MINUTES", "5")))
    parser.add_argument("--no-fresh", action="store_true", help="keep existing data instead of starting empty")
    parser.add_argument("--keep-stack", action="store_true", help="leave the chaos stack running afterwards")
    args = parser.parse_args()
    os.chdir(ROOT)

    if not args.no_fresh:
        log("starting a fresh stack with fault injection on")
        subprocess.run(COMPOSE + ["down", "-v"], capture_output=True)
    subprocess.run(COMPOSE + ["up", "-d", "--wait"], capture_output=True, check=True)
    wait_until("cluster ready", lambda: (not status()["safe_mode"]
                                         and sum(w["has_reported"] for w in status()["workers"]) == 4, None), 120)

    started = time.monotonic()
    asyncio.run(run(args.minutes))
    restore_everything()
    log("disruption over after %.0f s; checking convergence" % (time.monotonic() - started))

    checks = verify(None)
    print()
    print("=" * 100)
    print("CHAOS RUN SUMMARY  (%.1f min of load, faults: delay 300 ms, drop 5 %%, dup 5 %%)" % args.minutes)
    print("-" * 100)
    for key, value in stats.items():
        print("  %-12s %d" % (key, value))
    print("-" * 100)
    passed = 0
    for name, ok, detail in checks:
        passed += ok
        print("  %s  %s" % ("PASS" if ok else "FAIL", name))
        if not ok:
            print("        " + json.dumps(detail, default=str)[:400])
    print("-" * 100)
    print("  %d/%d checks passed" % (passed, len(checks)))
    print("=" * 100)

    if not args.keep_stack:
        log("restoring the normal stack (fault injection off, data kept)")
        subprocess.run(["docker", "compose", "up", "-d", "--wait"], capture_output=True)
    sys.exit(0 if passed == len(checks) else 1)


if __name__ == "__main__":
    main()

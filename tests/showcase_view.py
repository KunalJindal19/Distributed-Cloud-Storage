#!/usr/bin/env python3
"""
Views of the object store's inner state for showcase.sh, read from the REST API (standard library only).

    showcase_view.py grid [OBJECT_ID ...]     where every copy of every chunk is (chunks x workers);
                                              without ids, the 6 newest objects
    showcase_view.py follow repaired WORKER TIMEOUT [OBJECT_ID ...]
                                              until WORKER is DEAD and every chunk has RF live copies again
    showcase_view.py follow repaired-files WORKER TIMEOUT OBJECT_ID ...
                                              until WORKER is DEAD and the given objects' chunks have RF live
                                              copies again (the rest of the store may still be repairing)
    showcase_view.py follow healthy WORKER TIMEOUT [OBJECT_ID ...]
                                              until every worker is ALIVE and every chunk has exactly RF copies
          (follow prints a line whenever the worker's state or the given objects' copies change, and every
           10 s otherwise; exit 0 when reached, 1 on time-out)
    showcase_view.py check healthy            exit 0 if every worker is ALIVE and every chunk has exactly RF copies
    showcase_view.py check fsck               exit 0 if fsck finds every disk matching the controller's map
    showcase_view.py copies WORKER            the number of chunk copies the controller records on WORKER
    showcase_view.py objects                  the number of objects stored

Environment: BASE_URL (default http://localhost:8000).
"""

import json
import os
import sys
import time
import urllib.request

API = os.environ.get("BASE_URL", "http://localhost:8000").rstrip("/") + "/api/v1"
TTY = sys.stdout.isatty() and os.environ.get("NO_COLOR") is None
WIDTH = 9
STATE_COLOR = {"ALIVE": "32", "STALE": "33", "DEAD": "31"}


def color(code, text):
    return f"\033[{code}m{text}\033[0m" if TTY and code else text


def cell(mark, code, width=WIDTH):
    """A mark centred in a column, colouring only the mark so the padding stays right."""
    left = (width - len(mark)) // 2
    return " " * left + color(code, mark) + " " * (width - len(mark) - left)


def get(path, timeout=10):
    with urllib.request.urlopen(API + path, timeout=timeout) as response:
        return json.loads(response.read())


def ordered_workers(status):
    def number(worker):
        digits = "".join(c for c in worker["worker_id"] if c.isdigit())
        return (int(digits) if digits else 0, worker["worker_id"])
    return sorted(status["workers"], key=number)


def live_copies(chunk):
    return sum(1 for loc in chunk["locations"] if loc["status"] == "OK" and loc["worker_status"] != "DEAD")


# ---- grid -------------------------------------------------------------------------------------

def grid(object_ids):
    status = get("/cluster/status")
    rf = status["config"]["replication_factor"]
    workers = ordered_workers(status)
    ids = [w["worker_id"] for w in workers]
    if not object_ids:
        object_ids = [o["object_id"] for o in get("/objects?limit=6")["items"]]
    rule = "  " + "─" * (31 + WIDTH * len(ids))
    print("  " + "object".ljust(24) + "chunk ".rjust(7) + "".join(i.center(WIDTH) for i in ids))
    print("  " + " " * 31 + "".join(cell(w["state"], STATE_COLOR.get(w["state"])) for w in workers))
    print(rule)
    short_of = surplus = 0
    for object_id in object_ids:
        try:
            meta = get(f"/objects/{object_id}/metadata")
        except OSError:
            print("  " + object_id[:23].ljust(24) + "  (deleted)")
            continue
        name = meta["name"] if len(meta["name"]) <= 23 else meta["name"][:22] + "…"
        if not meta["chunks"]:
            print("  " + name.ljust(24) + "-".rjust(6) + "  (empty object, no chunks)")
        for chunk in meta["chunks"]:
            marks = {}
            for loc in chunk["locations"]:
                if loc["status"] == "OK":
                    marks[loc["worker_id"]] = ("●", "2" if loc["worker_status"] == "DEAD" else "32")
                elif loc["status"] == "CORRUPT":
                    marks[loc["worker_id"]] = ("✗", "31")
                else:
                    marks[loc["worker_id"]] = ("○", "2")
            live = live_copies(chunk)
            short_of += live < rf
            surplus += live > rf or any(m[0] == "○" for m in marks.values())
            note = "" if live == rf else color("33", f"  {live} live cop{'y' if live == 1 else 'ies'}")
            label = name if chunk["index"] == 0 else ""
            row = "".join(cell(*marks[i]) if i in marks else cell("·", "2") for i in ids)
            print("  " + label.ljust(24) + f"#{chunk['index']}".rjust(6) + " " + row + note)
    print(rule)
    print("  " + "copies held, all objects".ljust(31) + "".join(str(w["chunk_count"]).center(WIDTH) for w in workers))
    if short_of == 0 and surplus == 0:
        verdict = color("32", f"every chunk shown has exactly {rf} live copies")
    else:
        verdict = color("33", f"{short_of} chunk(s) short of copies · {surplus} with a surplus copy")
    print(f"  ● copy   ○ surplus copy, being deleted   ✗ corrupt copy   · no copy      {verdict}")


# ---- follow -----------------------------------------------------------------------------------

def mine(object_ids, rf):
    """(chunks short of copies, chunks with a surplus, chunks) over the given objects."""
    short_of = surplus = total = 0
    for object_id in object_ids:
        try:
            meta = get(f"/objects/{object_id}/metadata")
        except OSError:
            continue
        for chunk in meta["chunks"]:
            live = live_copies(chunk)
            total += 1
            short_of += live < rf
            surplus += live > rf or any(loc["status"] == "EXCESS" for loc in chunk["locations"])
    return short_of, surplus, total


def reached(kind, s, victim, files_short=0):
    copies_ok = s["under_replicated"] == 0 and s["missing_chunks"] == 0 and s["pending_replications"] == 0
    if kind in ("repaired", "repaired-files"):
        state = next((w["state"] for w in s["workers"] if w["worker_id"] == victim), None)
        enough = copies_ok if kind == "repaired" else files_short == 0
        return state == "DEAD" and enough and not s["safe_mode"]
    clean = (s["over_replicated"] == 0 and s["replicas"]["excess"] == 0 and s["replicas"]["corrupt"] == 0
             and sum((s.get("ledger") or {}).values()) == 0)
    everyone = all(w["state"] == "ALIVE" and w["has_reported"] and not w.get("resyncing") for w in s["workers"])
    return everyone and copies_ok and clean and not s["safe_mode"]


def follow(kind, victim, timeout, object_ids):
    started = time.monotonic()
    last_key, last_print = None, -1e9
    while True:
        elapsed = time.monotonic() - started
        try:
            s = get("/cluster/status")
            rf = s["config"]["replication_factor"]
            w = next((w for w in s["workers"] if w["worker_id"] == victim), None)
            state = "unknown" if w is None else w["state"]
            if w is not None and w["state"] == "ALIVE" and (not w["has_reported"] or w.get("resyncing")):
                state += " (registered, resyncing)"
            short_of, surplus, total = mine(object_ids, rf)
            key = (state, short_of, surplus)
            if key != last_key or elapsed - last_print >= 10:
                deletions = sum((s.get("ledger") or {}).values())
                part = f" · showcase files: {short_of} of {total} chunks short, {surplus} with a surplus" if object_ids else ""
                print(f"  {color('2', f'+{elapsed:4.0f}s')}  {victim} {color(STATE_COLOR.get(state.split()[0]), state)}{part}"
                      f" · whole store: {s['under_replicated']} short, {s['pending_replications']} on the way,"
                      f" {s['over_replicated']} surplus, {deletions} deletions", flush=True)
                last_key, last_print = key, elapsed
            if reached(kind, s, victim, short_of):
                rest = s["under_replicated"] + s["pending_replications"]
                more = (f" (the store's other {s['under_replicated']} chunks short of a copy keep being copied in the"
                        f" background)" if rest else "")
                print(f"  {color('32', '✓')} reached after {elapsed:.0f} s{more}", flush=True)
                return 0
        except OSError as e:
            if elapsed - last_print >= 10:
                print(f"  +{elapsed:4.0f}s  (the controller cannot be reached: {e})", flush=True)
                last_print = elapsed
        if elapsed >= timeout:
            print(f"  {color('31', f'✗ not reached within {timeout:.0f} s')}", flush=True)
            return 1
        time.sleep(1)


# ---- checks and numbers -------------------------------------------------------------------------

def check(kind):
    if kind == "fsck":
        f = get("/fsck", timeout=60)
        mismatches = sum(len(v) for v in (f.get("disk_vs_map") or {}).values())
        return 0 if (f["status"] == "HEALTHY" and f.get("quiescent") and f.get("orphans_on_disk") == 0
                     and mismatches == 0) else 1
    return 0 if reached("healthy", get("/cluster/status"), None) else 1


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return 64
    command, args = sys.argv[1], sys.argv[2:]
    try:
        if command == "grid":
            grid(args)
        elif command == "follow" and len(args) >= 3:
            return follow(args[0], args[1], float(args[2]), args[3:])
        elif command == "check" and args:
            return check(args[0])
        elif command == "copies" and args:
            print(next((w["chunk_count"] for w in get("/cluster/status")["workers"] if w["worker_id"] == args[0]), 0))
        elif command == "objects":
            print(get("/cluster/status")["objects"])
        else:
            print(__doc__)
            return 64
    except OSError as e:
        if command == "check":
            return 1
        print(f"  (the controller cannot be reached: {e})")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

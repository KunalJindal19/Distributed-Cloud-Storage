SHELL := /bin/bash
# The controller's port on this machine: docker-compose.yml publishes it as ${CONTROLLER_PORT:-9000}.
# Exported, so "make up" and every script below use the same port.
CONTROLLER_PORT ?= $(shell grep -m1 -oE 'CONTROLLER_PORT:-[0-9]+' docker-compose.yml | cut -d- -f2)
export CONTROLLER_PORT
BASE  ?= http://localhost:$(CONTROLLER_PORT)
# The admin account (sees every object). docker-compose.yml, the scripts and the targets below all
# use these, so set them here or in the environment to change the admin password everywhere.
ADMIN_EMAIL    ?= admin@gmail.com
ADMIN_PASSWORD ?= admin123
export ADMIN_EMAIL ADMIN_PASSWORD
ADMIN_CURL := curl -fsS -u "$(ADMIN_EMAIL):$(ADMIN_PASSWORD)"

# Published images: $(REPO)/objectstore-{controller,worker,client}:$(VERSION). The defaults come from
# docker-compose.yml, so the version lives in one place. Bump it there before every "make publish".
REPO      ?= $(shell grep -m1 -oE 'OBJECTSTORE_REPO:-[^}]+' docker-compose.yml | cut -d- -f2-)
VERSION   ?= $(shell grep -m1 -oE 'OBJECTSTORE_VERSION:-[^}]+' docker-compose.yml | cut -d- -f2-)
PLATFORMS ?= linux/amd64,linux/arm64
# Build from this checkout (docker-compose.build.yml) instead of downloading the published images.
COMPOSE_BUILD := docker compose -f docker-compose.yml -f docker-compose.build.yml

.DEFAULT_GOAL := help
.PHONY: help pull publish build up down down-v restart ps logs status fsck app dashboard stop start pause unpause where corrupt corrupt-meta rmchunk rmmeta quarantine test test-scale scenarios chaos demo showcase showcase-up showcase-logs

help:            ## show this help
	@grep -E '^[a-z-]+: .*## ' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  make %-14s %s\n", $$1, $$2}'

pull:            ## download the published images (what "docker compose up" does on any machine)
	docker compose pull

publish:         ## build for amd64 + arm64 and push to Docker Hub (needs "docker login"; bump the version first)
	@if docker manifest inspect $(REPO)/objectstore-controller:$(VERSION) > /dev/null 2>&1; then \
		echo "$(REPO)/objectstore-*:$(VERSION) is already published; bump OBJECTSTORE_VERSION in docker-compose.yml first"; exit 1; fi
	@for target in controller worker client; do \
		echo "### $(REPO)/objectstore-$$target:$(VERSION) for $(PLATFORMS)"; \
		docker buildx build --platform $(PLATFORMS) --target $$target \
			-t $(REPO)/objectstore-$$target:$(VERSION) --push . || exit 1; \
	done

build:           ## build the three images from this checkout (controller, worker, client)
	$(COMPOSE_BUILD) build

up:              ## build from this checkout and start everything; returns when all containers are healthy
	$(COMPOSE_BUILD) up --build -d --wait
	@docker compose ps
	@echo
	@echo "API:        $(BASE)/api/v1/objects"
	@echo "Web client: $(BASE)/app"
	@echo "Dashboard:  $(BASE)/dashboard"

down:            ## stop and remove the containers (volumes, i.e. all data, are kept)
	docker compose down

down-v:          ## stop and wipe everything, including all stored data
	docker compose down -v

restart:         ## restart the controller only (workers keep running)
	docker compose restart controller

ps:              ## container status
	docker compose ps

logs:            ## follow the logs of all containers
	docker compose logs -f --tail=100

status:          ## cluster status as JSON (as the admin)
	@$(ADMIN_CURL) $(BASE)/api/v1/cluster/status | ( jq . 2>/dev/null || cat )

fsck:            ## end-to-end consistency check (disks vs. the controller's block map)
	@$(ADMIN_CURL) $(BASE)/api/v1/fsck | ( jq . 2>/dev/null || cat )

app:             ## open the web client in a browser
	@( open $(BASE)/app || xdg-open $(BASE)/app ) 2>/dev/null || echo "open $(BASE)/app"

dashboard:       ## open the dashboard in a browser
	@( open $(BASE)/dashboard || xdg-open $(BASE)/dashboard ) 2>/dev/null || echo "open $(BASE)/dashboard"

stop:            ## simulate a worker failure:   make stop W=worker2
	docker stop $(W)

start:           ## bring the worker back:       make start W=worker2
	docker start $(W)

pause:           ## freeze a container (STALE):   make pause W=worker2
	docker pause $(W)

unpause:         ## unfreeze it:                  make unpause W=worker2
	docker unpause $(W)

# Fault injection on one stored copy. Find chunk ids and their holders with "make where O=<object_id>".
# Touch one copy of a chunk at a time: the system repairs it from the other one. If both copies are
# gone or corrupt, that chunk (and its object) is lost.
need_wc = @if [ -z "$(W)" ] || [ -z "$(C)" ]; then echo "usage: make $@ W=<worker> C=<chunk_id>   (find them with: make where O=<object_id>)"; exit 2; fi

where:           ## chunks of an object and their holders (as the admin): make where O=<object_id>
	@if [ -z "$(O)" ]; then echo "usage: make where O=<object_id>   (object ids: ./oscli list)"; exit 2; fi
	OSCLI_EMAIL="$(ADMIN_EMAIL)" OSCLI_PASSWORD="$(ADMIN_PASSWORD)" ./oscli where $(O)

corrupt:         ## damage a stored chunk:       make corrupt W=worker3 C=<chunk_id>
	$(need_wc)
	@# Overwrites 4 bytes in the middle of the data file; its size and its .meta sidecar do not change.
	docker exec $(W) sh -c 'printf "XXXX" | dd of=/data/chunks/$(C) bs=1 seek=100 conv=notrunc status=none && echo "corrupted $(C) on $(W)"'

corrupt-meta:    ## damage a chunk's checksum file: make corrupt-meta W=worker3 C=<chunk_id>
	$(need_wc)
	@# Writes a wrong checksum (with the right size) into the .meta sidecar; the data file is untouched.
	docker exec $(W) sh -c 'test -f /data/chunks/$(C) && printf "%064d %s\n" 0 $$(stat -c %s /data/chunks/$(C)) > /data/chunks/$(C).meta && echo "corrupted the .meta of $(C) on $(W)"'

rmchunk:         ## lose a stored copy:          make rmchunk W=worker2 C=<chunk_id>
	$(need_wc)
	@# Deletes the data file and its .meta sidecar, as a lost disk would; the scanner reports it missing.
	docker exec $(W) sh -c 'rm /data/chunks/$(C) /data/chunks/$(C).meta && echo "deleted $(C) (data and .meta) on $(W)"'

rmmeta:          ## lose only a checksum file:   make rmmeta W=worker2 C=<chunk_id>
	$(need_wc)
	@# The copy can no longer be verified, so the scanner quarantines it.
	docker exec $(W) sh -c 'rm /data/chunks/$(C).meta && echo "deleted the .meta of $(C) on $(W)"'

quarantine:      ## list a worker's quarantined copies: make quarantine W=worker2
	@if [ -z "$(W)" ]; then echo "usage: make quarantine W=<worker>"; exit 2; fi
	@docker exec $(W) sh -c 'ls -l /data/quarantine | tail -n +2; echo "$$(ls /data/quarantine | wc -l) quarantined file(s) on $(W)"'

test:            ## unit tests + in-process system tests (JDK 21+ and Maven, or Docker)
	@if command -v mvn > /dev/null 2>&1; then \
		mvn -B -ntp verify; \
	else \
		docker run --rm -v "$(CURDIR)":/workspace -v objectstore_m2:/root/.m2 -w /workspace \
			maven:3.9-eclipse-temurin-21 mvn -B -ntp verify; \
	fi

test-scale:      ## the scale test: 500,000 chunks, 8 workers, no HTTP (a few minutes)
	mvn -B -ntp -Pscale test -pl controller -am

scenarios:       ## the acceptance-criteria scripts against Docker (about 15 min; wipes data)
	@for script in tests/scenarios/block*.sh; do echo "### $$script"; BASE_URL=$(BASE) $$script || exit 1; done

chaos:           ## chaos test: faults on, disruptions, then convergence checks (CHAOS_MINUTES=5)
	BASE_URL=$(BASE) python3 tests/chaos.py

demo:            ## run the seven demonstrations against the running stack
	BASE_URL=$(BASE) ./demo.sh

showcase:        ## guided tour: inner state, worker2 stopped and started again (--no-pause: ARGS=--no-pause)
	BASE_URL=$(BASE) ./showcase.sh $(ARGS)

showcase-up:     ## start the stack printing its inner state (every message, state every 10 s)
	docker compose -f docker-compose.yml -f docker-compose.build.yml -f docker-compose.showcase.yml up --build -d --wait

showcase-logs:   ## follow the controller and the four workers, with the showcase output
	docker compose -f docker-compose.yml -f docker-compose.showcase.yml logs -f --tail 40 controller worker1 worker2 worker3 worker4

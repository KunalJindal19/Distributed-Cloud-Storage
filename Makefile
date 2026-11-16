SHELL := /bin/bash
BASE  ?= http://localhost:8000

.DEFAULT_GOAL := help
.PHONY: help build up down down-v restart ps logs status dashboard stop start corrupt test demo

help:            ## show this help
	@grep -E '^[a-z-]+: .*## ' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  make %-10s %s\n", $$1, $$2}'

build:           ## build the three images (controller, worker, client)
	docker compose build

up:              ## build and start everything; returns when all containers are healthy
	docker compose up --build -d --wait
	@docker compose ps
	@echo
	@echo "API:        $(BASE)/api/v1/objects"
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

status:          ## cluster status as JSON
	@curl -fsS $(BASE)/api/v1/cluster/status | ( jq . 2>/dev/null || cat )

dashboard:       ## open the dashboard in a browser
	@( open $(BASE)/dashboard || xdg-open $(BASE)/dashboard ) 2>/dev/null || echo "open $(BASE)/dashboard"

stop:            ## simulate a worker failure:   make stop W=worker2
	docker stop $(W)

start:           ## bring the worker back:       make start W=worker2
	docker start $(W)

corrupt:         ## damage a stored chunk:       make corrupt W=worker3 C=<chunk_id>
	@# Overwrites 4 bytes in the middle of the file; its size does not change.
	docker exec $(W) sh -c 'printf "XXXX" | dd of=/data/chunks/$(C) bs=1 seek=100 conv=notrunc status=none && echo "corrupted $(C) on $(W)"'

test:            ## run all unit and integration tests (JDK 21+ and Maven, or Docker)
	@if command -v mvn > /dev/null 2>&1; then \
		mvn -B -ntp verify; \
	else \
		docker run --rm -v "$(CURDIR)":/workspace -v objectstore_m2:/root/.m2 -w /workspace \
			maven:3.9-eclipse-temurin-21 mvn -B -ntp verify; \
	fi

demo:            ## run the six required demonstrations against the running stack
	BASE_URL=$(BASE) ./demo.sh

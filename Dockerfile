# syntax=docker/dockerfile:1.7
#
# One Dockerfile, three images (build targets): controller, worker, client.
# The Maven build runs once in a shared stage; each runtime image only receives its own jar.

# ---- build: compile and package all modules -------------------------------------------------
# Runs on the machine's own architecture even when building for another one ("make publish" builds
# linux/amd64 and linux/arm64): the jars are the same for both, so Maven never runs under emulation.
FROM --platform=$BUILDPLATFORM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /workspace

COPY pom.xml ./
COPY common/pom.xml common/pom.xml
COPY worker/pom.xml worker/pom.xml
COPY controller/pom.xml controller/pom.xml
COPY client/pom.xml client/pom.xml
COPY system-tests/pom.xml system-tests/pom.xml
COPY common/src common/src
COPY worker/src worker/src
COPY controller/src controller/src
COPY client/src client/src

# Tests are run by "make test" (and in CI), not while building images.
# The cache mount keeps downloaded dependencies between builds without baking them into a layer.
RUN --mount=type=cache,target=/root/.m2 mvn -B -ntp -DskipTests -pl common,worker,controller,client package

# ---- runtime base: JRE, non-root user, /data owned by that user ---------------------------------
FROM eclipse-temurin:21-jre AS runtime
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl jq \
 && rm -rf /var/lib/apt/lists/* \
 && groupadd --system --gid 10001 objectstore \
 && useradd --system --uid 10001 --gid objectstore --home-dir /app --shell /usr/sbin/nologin objectstore \
 && mkdir -p /app /data \
 && chown -R objectstore:objectstore /app /data
WORKDIR /app
ENV JAVA_OPTS="-XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError"

# ---- controller ---------------------------------------------------------------------------------
FROM runtime AS controller
COPY --from=build --chown=objectstore:objectstore /workspace/controller/target/controller-exec.jar /app/app.jar
USER objectstore
ENV PORT=8000
EXPOSE 8000
# A named volume mounted here inherits the ownership set above, so the non-root user can write to it.
VOLUME ["/data"]
# "exec" makes the JVM PID 1, so "docker stop" delivers SIGTERM to it and shutdown is graceful.
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar"]

# ---- worker -------------------------------------------------------------------------------------
FROM runtime AS worker
COPY --from=build --chown=objectstore:objectstore /workspace/worker/target/worker-exec.jar /app/app.jar
USER objectstore
# Each worker listens on its own port (8001-8004), set through the PORT variable in docker-compose.yml.
ENV PORT=8001
VOLUME ["/data"]
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar"]

# ---- client: the CLI plus curl and jq, kept running so commands can be "docker exec"-ed ---------
FROM runtime AS client
COPY --from=build /workspace/client/target/client.jar /app/client.jar
RUN printf '%s\n' '#!/bin/sh' \
      'exec java -XX:TieredStopAtLevel=1 -XX:+UseSerialGC -Xshare:auto -jar /app/client.jar "$@"' \
      > /usr/local/bin/oscli \
 && chmod 0755 /usr/local/bin/oscli
USER objectstore
ENV BASE_URL=http://controller:8000
WORKDIR /work
CMD ["sleep", "infinity"]

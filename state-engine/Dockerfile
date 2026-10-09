# State engine image. Two entry points:
#   default: the whole pipeline in one process, in memory (local development)
#   worker:  docker run ... concord/state-engine java -cp /app/app.jar dev.concord.engine.worker.WorkerMain
# Build from the repository root:  docker build -f state-engine/Dockerfile -t concord/state-engine .
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY pom.xml .
COPY gateway-simulator/pom.xml gateway-simulator/
COPY ingestion/pom.xml ingestion/
COPY state-engine/pom.xml state-engine/
COPY gateway-simulator/src gateway-simulator/src
COPY ingestion/src ingestion/src
COPY state-engine/src state-engine/src
RUN mvn -B -q -pl state-engine -am package -DskipTests

FROM eclipse-temurin:21-jre
RUN useradd --system --uid 10001 concord
USER concord
WORKDIR /app
COPY --from=build /src/state-engine/target/state-engine-*-all.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-cp", "/app/app.jar"]
CMD ["dev.concord.engine.local.LocalConcord"]

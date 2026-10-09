# State engine SQS worker (ECS Fargate in AWS): QUEUE_URL, PAYMENTS_TABLE.
# Build from the repository root:  docker build -f state-engine/Dockerfile -t concord/state-engine .
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
# The whole reactor (minus build output, see .dockerignore): robust to new modules being added.
COPY . .
RUN mvn -B -q -pl state-engine -am package -DskipTests

FROM eclipse-temurin:21-jre
RUN useradd --system --uid 10001 concord
USER concord
WORKDIR /app
COPY --from=build /src/state-engine/target/state-engine-*-all.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-cp", "/app/app.jar", "dev.concord.engine.worker.WorkerMain"]

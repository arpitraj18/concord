# All of Concord in one process, in memory: local development and demos.
# Build from the repository root:  docker build -f local-runner/Dockerfile -t concord/local-runner .
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
# The whole reactor (minus build output, see .dockerignore): robust to new modules being added.
COPY . .
RUN mvn -B -q -pl local-runner -am package -DskipTests

FROM eclipse-temurin:21-jre
RUN useradd --system --uid 10001 concord
USER concord
WORKDIR /app
COPY --from=build /src/local-runner/target/local-runner-*-all.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/app.jar"]

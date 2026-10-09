# Sample merchant shop on PostgreSQL that consumes Concord events exactly once.
# Build from the repository root:  docker build -f sample-merchant/Dockerfile -t concord/sample-merchant .
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
# The whole reactor (minus build output, see .dockerignore): robust to new modules being added.
COPY . .
RUN mvn -B -q -pl sample-merchant -am package -DskipTests

FROM eclipse-temurin:21-jre
RUN useradd --system --uid 10001 concord
USER concord
WORKDIR /app
COPY --from=build /src/sample-merchant/target/sample-merchant-*-exec.jar app.jar
EXPOSE 8090
ENTRYPOINT ["java", "-jar", "/app/app.jar"]

# Fake payment gateway (A, B or C via SIM_GATEWAY).
# Build from the repository root:  docker build -f gateway-simulator/Dockerfile -t concord/gateway-simulator .
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
# The whole reactor (minus build output, see .dockerignore): robust to new modules being added.
COPY . .
RUN mvn -B -q -pl gateway-simulator -am package -DskipTests

FROM eclipse-temurin:21-jre
RUN useradd --system --uid 10001 concord
USER concord
WORKDIR /app
COPY --from=build /src/gateway-simulator/target/gateway-simulator-*-exec.jar app.jar
EXPOSE 8081
ENTRYPOINT ["java", "-XX:+UseZGC", "-jar", "/app/app.jar"]

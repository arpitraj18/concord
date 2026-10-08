# Build from the repository root:  docker build -f gateway-simulator/Dockerfile -t concord/gateway-simulator .
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY pom.xml .
COPY gateway-simulator/pom.xml gateway-simulator/
RUN mvn -B -q -pl gateway-simulator -am dependency:go-offline
COPY gateway-simulator/src gateway-simulator/src
RUN mvn -B -q -pl gateway-simulator -am package -DskipTests

FROM eclipse-temurin:21-jre
RUN useradd --system --uid 10001 sim
USER sim
WORKDIR /app
COPY --from=build /src/gateway-simulator/target/gateway-simulator-*.jar app.jar
EXPOSE 8081
ENTRYPOINT ["java", "-XX:+UseZGC", "-jar", "/app/app.jar"]

# Local ingestion server (same code as the Lambda). Build from the repository root:
#   docker build -f ingestion/Dockerfile -t concord/ingestion .
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY pom.xml .
COPY gateway-simulator/pom.xml gateway-simulator/
COPY ingestion/pom.xml ingestion/
COPY gateway-simulator/src gateway-simulator/src
COPY ingestion/src ingestion/src
RUN mvn -B -q -pl ingestion -am package -Dmaven.test.skip=true

FROM eclipse-temurin:21-jre
RUN useradd --system --uid 10001 concord
USER concord
WORKDIR /app
COPY --from=build /src/ingestion/target/ingestion-*-lambda.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-cp", "/app/app.jar", "dev.concord.ingest.local.LocalIngestServer"]

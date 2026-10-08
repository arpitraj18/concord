package dev.concord.ingest.aws;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

/** S3 has no in-process fake, so this one needs Docker. It is skipped where Docker is unavailable. */
@Testcontainers(disabledWithoutDocker = true)
class S3RawArchiveTest {

    @Container
    static final GenericContainer<?> S3 = new GenericContainer<>("adobe/s3mock:4.7.0")
            .withExposedPorts(9090);

    @Test
    void storesObjectUnderGivenKey() {
        try (S3Client s3 = S3Client.builder()
                .endpointOverride(URI.create("http://" + S3.getHost() + ":" + S3.getMappedPort(9090)))
                .region(Region.US_EAST_1)
                .forcePathStyle(true)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .build()) {
            s3.createBucket(b -> b.bucket("concord-raw-test"));
            new S3RawArchive(s3, "concord-raw-test").put("raw/gateway=A/x.json", "{\"a\":1}".getBytes(StandardCharsets.UTF_8));
            String stored = s3.getObjectAsBytes(b -> b.bucket("concord-raw-test").key("raw/gateway=A/x.json")).asUtf8String();
            assertThat(stored).isEqualTo("{\"a\":1}");
        }
    }
}

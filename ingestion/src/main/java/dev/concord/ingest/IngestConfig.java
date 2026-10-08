package dev.concord.ingest;

import dev.concord.ingest.verify.Secrets;
import java.time.Duration;
import java.util.EnumMap;
import java.util.Map;

/**
 * Settings, read from environment variables so the same code runs in Lambda and locally.
 *
 * <pre>
 * GATEWAY_A_SECRETS, GATEWAY_B_SECRETS, GATEWAY_C_SECRETS  comma-separated; a gateway without secrets is disabled
 * SIGNATURE_TOLERANCE_SECONDS   default 300
 * DEDUP_LEASE_SECONDS           default 30
 * DEDUP_TTL_HOURS               default 168 (must outlast the longest gateway retry schedule)
 * ARCHIVE_BUCKET, DEDUP_TABLE, QUEUE_URL   required for the AWS backend
 * AWS_ENDPOINT_URL              optional, e.g. LocalStack; read by the AWS SDK itself
 * </pre>
 */
public record IngestConfig(
        Map<Gateway, Secrets> secrets,
        Duration signatureTolerance,
        Duration dedupLease,
        Duration dedupTtl,
        String archiveBucket,
        String dedupTable,
        String queueUrl) {

    public static IngestConfig fromEnv(Map<String, String> env) {
        Map<Gateway, Secrets> secrets = new EnumMap<>(Gateway.class);
        for (Gateway g : Gateway.values()) {
            String csv = env.get("GATEWAY_" + g + "_SECRETS");
            if (csv != null && !csv.isBlank()) {
                secrets.put(g, Secrets.parse(csv));
            }
        }
        return new IngestConfig(
                secrets,
                Duration.ofSeconds(longOr(env, "SIGNATURE_TOLERANCE_SECONDS", 300)),
                Duration.ofSeconds(longOr(env, "DEDUP_LEASE_SECONDS", 30)),
                Duration.ofHours(longOr(env, "DEDUP_TTL_HOURS", 168)),
                env.get("ARCHIVE_BUCKET"),
                env.get("DEDUP_TABLE"),
                env.get("QUEUE_URL"));
    }

    private static long longOr(Map<String, String> env, String name, long fallback) {
        String v = env.get(name);
        return v == null || v.isBlank() ? fallback : Long.parseLong(v.trim());
    }

    public void requireAwsSettings() {
        if (archiveBucket == null || dedupTable == null || queueUrl == null) {
            throw new IllegalStateException("ARCHIVE_BUCKET, DEDUP_TABLE and QUEUE_URL must be set");
        }
    }
}

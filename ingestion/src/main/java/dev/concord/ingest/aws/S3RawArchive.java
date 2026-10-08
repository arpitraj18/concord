package dev.concord.ingest.aws;

import dev.concord.ingest.archive.RawArchive;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;

public final class S3RawArchive implements RawArchive {

    private final S3Client s3;
    private final String bucket;

    public S3RawArchive(S3Client s3, String bucket) {
        this.s3 = s3;
        this.bucket = bucket;
    }

    @Override
    public void put(String key, byte[] json) {
        s3.putObject(b -> b.bucket(bucket).key(key).contentType("application/json"), RequestBody.fromBytes(json));
    }
}

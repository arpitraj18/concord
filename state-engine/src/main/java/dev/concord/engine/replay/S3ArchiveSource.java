package dev.concord.engine.replay;

import java.util.function.BiConsumer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.S3Object;

public final class S3ArchiveSource implements ArchiveSource {

    private final S3Client s3;
    private final String bucket;

    public S3ArchiveSource(S3Client s3, String bucket) {
        this.s3 = s3;
        this.bucket = bucket;
    }

    @Override
    public void forEach(String prefix, BiConsumer<String, byte[]> consumer) {
        ListObjectsV2Request request = ListObjectsV2Request.builder().bucket(bucket).prefix(prefix).build();
        for (S3Object o : s3.listObjectsV2Paginator(request).contents()) {
            consumer.accept(o.key(), s3.getObjectAsBytes(b -> b.bucket(bucket).key(o.key())).asByteArray());
        }
    }
}

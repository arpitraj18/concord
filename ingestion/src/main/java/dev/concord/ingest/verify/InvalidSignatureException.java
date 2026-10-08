package dev.concord.ingest.verify;

public class InvalidSignatureException extends RuntimeException {
    public InvalidSignatureException(String reason) {
        super(reason);
    }
}

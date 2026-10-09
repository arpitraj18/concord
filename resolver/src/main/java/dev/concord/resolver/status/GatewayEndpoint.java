package dev.concord.resolver.status;

/** Where one gateway's API lives and the read-only key to call it with. */
public record GatewayEndpoint(String baseUrl, String apiKey) {
}

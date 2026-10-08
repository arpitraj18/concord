package dev.concord.ingest.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPResponse;
import dev.concord.ingest.Gateway;
import dev.concord.ingest.InboundWebhook;
import dev.concord.ingest.IngestConfig;
import dev.concord.ingest.IngestResult;
import dev.concord.ingest.IngestionService;
import dev.concord.ingest.Wiring;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.Base64;
import java.util.Map;
import java.util.Optional;

/**
 * API Gateway HTTP API (payload v2) -> ingestion. Route: {@code POST /webhooks/{gateway}}.
 *
 * <p>The service is built once per Lambda container (static init), so warm invocations reuse the AWS
 * clients and their connections.
 */
public final class IngestHandler implements RequestHandler<APIGatewayV2HTTPEvent, APIGatewayV2HTTPResponse> {

    private final IngestionService service;
    private final Clock clock;

    /** Used by the Lambda runtime. */
    public IngestHandler() {
        this(Holder.SERVICE, Clock.systemUTC());
    }

    public IngestHandler(IngestionService service, Clock clock) {
        this.service = service;
        this.clock = clock;
    }

    @Override
    public APIGatewayV2HTTPResponse handleRequest(APIGatewayV2HTTPEvent event, Context context) {
        String segment = event.getPathParameters() == null ? null : event.getPathParameters().get("gateway");
        Optional<Gateway> gateway = Gateway.fromPath(segment);
        if (gateway.isEmpty()) {
            return respond(IngestResult.UNKNOWN_GATEWAY);
        }
        String body = event.getBody() == null ? "" : event.getBody();
        // API Gateway passes text bodies through unchanged and base64-encodes binary ones. The signature
        // is over the original bytes, so decode instead of re-encoding the string.
        byte[] bytes = event.getIsBase64Encoded() ? Base64.getDecoder().decode(body) : body.getBytes(StandardCharsets.UTF_8);
        Map<String, String> headers = event.getHeaders() == null ? Map.of() : event.getHeaders();
        return respond(service.ingest(new InboundWebhook(gateway.get(), headers, bytes, clock.instant())));
    }

    private static APIGatewayV2HTTPResponse respond(IngestResult result) {
        return APIGatewayV2HTTPResponse.builder()
                .withStatusCode(result.httpStatus())
                .withHeaders(Map.of("Content-Type", "application/json"))
                .withBody("{\"result\":\"" + result + "\"}")
                .build();
    }

    private static final class Holder {
        static final IngestionService SERVICE = Wiring.aws(IngestConfig.fromEnv(System.getenv()));
    }
}

package dev.concord.engine.mapping;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.concord.engine.machine.CanonicalState;
import dev.concord.ingest.Gateway;
import dev.concord.ingest.queue.IngestedEvent;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.EnumMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** Applies the active mapping of each gateway to ingested events. */
public final class Normalizer {

    private final Map<Gateway, GatewayMapping> mappings;
    private final ObjectMapper json;

    public Normalizer(Map<Gateway, GatewayMapping> mappings, ObjectMapper json) {
        mappings.values().forEach(GatewayMapping::validate);
        this.mappings = new EnumMap<>(mappings);
        this.json = json;
    }

    /** The mappings shipped with the engine: {@code mappings/gateway-<x>.v1.json}. */
    public static Normalizer withBundledMappings(ObjectMapper json) {
        Map<Gateway, GatewayMapping> m = new EnumMap<>(Gateway.class);
        for (Gateway g : Gateway.values()) {
            String resource = "/mappings/gateway-" + g.name().toLowerCase(Locale.ROOT) + ".v1.json";
            try (InputStream in = Normalizer.class.getResourceAsStream(resource)) {
                if (in == null) {
                    throw new IllegalStateException("missing " + resource);
                }
                m.put(g, json.readValue(in, GatewayMapping.class));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return new Normalizer(m, json);
    }

    public GatewayMapping mapping(Gateway gateway) {
        return mappings.get(gateway);
    }

    public CanonicalEvent normalize(IngestedEvent event) {
        Gateway gateway = Gateway.valueOf(event.gateway());
        GatewayMapping mapping = mappings.get(gateway);
        if (mapping == null) {
            throw new InvalidMappingException("no mapping for gateway " + gateway);
        }
        JsonNode body;
        try {
            body = json.readTree(event.body());
        } catch (IOException e) {
            throw new InvalidMappingException("ingested body is not JSON: " + event.dedupKey());
        }
        CanonicalState claimed = claimedState(mapping, body);
        GatewayMapping.Fields f = mapping.fields();
        String currency = f == null ? null : upper(text(body, f.currency()));
        return new CanonicalEvent(
                gateway,
                event.paymentId(),
                event.dedupKey(),
                event.eventType(),
                claimed,
                f == null ? null : amountMinor(body, f.amount(), currency),
                currency,
                f == null ? null : text(body, f.orderId()),
                f == null ? null : instant(body, f.occurredAt()),
                Instant.parse(event.receivedAt()),
                mapping.id(),
                event.archiveKey());
    }

    static CanonicalState claimedState(GatewayMapping mapping, JsonNode body) {
        String type = body.at(mapping.eventType()).asText(null);
        for (GatewayMapping.Rule rule : mapping.rules()) {
            if (!rule.eventType().equals(type)) {
                continue;
            }
            if (rule.when() == null || rule.when().equals().equals(body.at(rule.when().pointer()).asText(null))) {
                return rule.state();
            }
        }
        return null;
    }

    private static JsonNode first(JsonNode body, GatewayMapping.Field field) {
        if (field == null || field.pointers() == null) {
            return null;
        }
        for (String pointer : field.pointers()) {
            JsonNode n = body.at(pointer);
            if (!n.isMissingNode() && !n.isNull()) {
                return n;
            }
        }
        return null;
    }

    private static String text(JsonNode body, GatewayMapping.Field field) {
        JsonNode n = first(body, field);
        return n == null ? null : n.asText();
    }

    private static Long amountMinor(JsonNode body, GatewayMapping.Field field, String currency) {
        JsonNode n = first(body, field);
        if (n == null) {
            return null;
        }
        BigDecimal value = new BigDecimal(n.asText());
        if ("MAJOR".equals(field.unit())) {
            int exponent = currency == null ? 2 : GatewayMapping.currencyExponents().getOrDefault(currency, 2);
            value = value.movePointRight(exponent);
        }
        return value.longValueExact();
    }

    private static Instant instant(JsonNode body, GatewayMapping.Field field) {
        JsonNode n = first(body, field);
        if (n == null) {
            return null;
        }
        return switch (field.format()) {
            case "EPOCH_SECONDS" -> Instant.ofEpochSecond(n.asLong());
            case "EPOCH_MILLIS" -> Instant.ofEpochMilli(n.asLong());
            default -> Instant.parse(n.asText());
        };
    }

    private static String upper(String s) {
        return s == null ? null : s.toUpperCase(Locale.ROOT);
    }

}

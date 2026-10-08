package dev.concord.simulator;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import dev.concord.simulator.model.Scenario;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;

public final class TestScenarios {

    public static final ObjectMapper JSON = JsonMapper.builder()
            .enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS)
            .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .findAndAddModules()
            .build();

    private TestScenarios() {
    }

    public static Scenario load(String name) {
        try {
            return JSON.readValue(Path.of("scenarios", name).toFile(), Scenario.class);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}

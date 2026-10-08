package dev.concord.simulator.model;

/** A scenario that cannot be simulated; reported to the caller as HTTP 400. */
public class ScenarioException extends RuntimeException {
    public ScenarioException(String message) {
        super(message);
    }
}

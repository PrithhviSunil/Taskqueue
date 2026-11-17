package com.prithhvi.taskqueue.worker;

import java.util.Map;
import java.util.Set;

import tools.jackson.databind.JsonNode;

import org.springframework.stereotype.Component;

/**
 * Maps a job type to the code that runs it. The queue doesn't care what a job does;
 * these are stand-ins so we can measure the queue itself.
 */
@Component
public class JobHandlers {

    @FunctionalInterface
    public interface Handler {
        void run(JsonNode payload) throws Exception;
    }

    private final Map<String, Handler> handlers = Map.of(
            // Does nothing: measures pure queue overhead (claim + complete round trips).
            "noop", payload -> { },

            // Simulates I/O-bound work like calling an email API: {"ms": 50}
            "sleep", payload -> Thread.sleep(payload.path("ms").asLong(50)),

            // Always throws: lets us test failure handling. {"message": "..."}
            "fail", payload -> {
                throw new RuntimeException(payload.path("message").asString("simulated failure"));
            }
    );

    public boolean supports(String type) {
        return handlers.containsKey(type);
    }

    public Set<String> types() {
        return handlers.keySet();
    }

    public Handler get(String type) {
        return handlers.get(type);
    }
}

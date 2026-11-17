package com.prithhvi.taskqueue.api;

import java.util.Map;

import com.prithhvi.taskqueue.queue.Job;
import com.prithhvi.taskqueue.queue.JobRepository;
import com.prithhvi.taskqueue.worker.JobHandlers;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import tools.jackson.databind.JsonNode;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class JobController {

    public record EnqueueRequest(
            @NotBlank String type,
            JsonNode payload,
            @Min(1) @Max(25) Integer maxAttempts
    ) {}

    public record EnqueueResponse(long id) {}

    private final JobRepository repo;
    private final JobHandlers handlers;

    public JobController(JobRepository repo, JobHandlers handlers) {
        this.repo = repo;
        this.handlers = handlers;
    }

    /**
     * Saves the job and returns immediately. 202 Accepted = "got it, will do it later",
     * which is exactly what a queue promises. The work itself happens in a worker.
     */
    @PostMapping("/jobs")
    public ResponseEntity<?> enqueue(@Valid @RequestBody EnqueueRequest req) {
        if (!handlers.supports(req.type())) {
            return ResponseEntity.badRequest()
                    .body(Map.of("error", "unknown job type: " + req.type(),
                                 "knownTypes", handlers.types()));
        }
        String payload = req.payload() == null ? "{}" : req.payload().toString();
        int maxAttempts = req.maxAttempts() == null ? 5 : req.maxAttempts();
        long id = repo.enqueue(req.type(), payload, maxAttempts);
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(new EnqueueResponse(id));
    }

    @GetMapping("/jobs/{id}")
    public ResponseEntity<Job> get(@PathVariable long id) {
        return repo.findById(id)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/stats")
    public Map<String, Long> stats() {
        return repo.stats();
    }

    /** Throughput and latency for jobs with ids in [from, to]. Used by bench/bench.py. */
    @GetMapping("/stats/window")
    public Map<String, Object> window(@RequestParam long from, @RequestParam long to) {
        return repo.window(from, to);
    }
}

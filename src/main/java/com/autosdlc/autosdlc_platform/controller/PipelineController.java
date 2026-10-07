package com.autosdlc.autosdlc_platform.controller;

import com.autosdlc.autosdlc_platform.model.dto.PipelineResponse;
import com.autosdlc.autosdlc_platform.model.dto.StartPipelineRequest;
import com.autosdlc.autosdlc_platform.model.dto.ApprovalRequest;
import com.autosdlc.autosdlc_platform.service.agents.ArchitectAgent;
import com.autosdlc.autosdlc_platform.service.agents.DeveloperAgent;
import com.autosdlc.autosdlc_platform.service.agents.TesterAgent;
import com.autosdlc.autosdlc_platform.service.agents.DevOpsAgent;
import com.autosdlc.autosdlc_platform.service.orchestrator.PipelineOrchestrator;
import com.autosdlc.autosdlc_platform.service.storage.ArtifactStorageService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequestMapping("/api/pipeline")
@RequiredArgsConstructor
public class PipelineController {

    private final PipelineOrchestrator orchestrator;
    private final ArchitectAgent architectAgent;
    private final DeveloperAgent developerAgent;
    private final TesterAgent testerAgent;
    private final DevOpsAgent devOpsAgent;
    private final ArtifactStorageService storageService;

    @PostMapping("/start")
    public ResponseEntity<PipelineResponse> startPipeline(@Valid @RequestBody StartPipelineRequest request) {
        PipelineResponse response = orchestrator.startPipeline(request);
        return ResponseEntity.ok(response);
    }

    @GetMapping("/{jobId}/status")
    public ResponseEntity<PipelineResponse> getStatus(@PathVariable UUID jobId) {
        return ResponseEntity.ok(orchestrator.getStatus(jobId));
    }

    /**
     * Human approval gate - called by the reviewer after an agent finishes and the job pauses
     * in AWAITING_APPROVAL (currentStage = &lt;AGENT&gt;_AWAITING_APPROVAL).
     *
     * Body: { "approved": true|false, "feedback": "..." }
     *   - approved=true  -> advance to the next agent (or mark COMPLETED after DevOps).
     *   - approved=false -> feedback is REQUIRED; the current agent re-runs with that feedback
     *                       injected into its LLM prompt, then pauses for approval again.
     *
     * Usage:
     *   POST /api/pipeline/{jobId}/approval
     *   Content-Type: application/json
     *   { "approved": true }
     *   -- or --
     *   { "approved": false, "feedback": "Make the requirements more detailed; add a login flow." }
     */
    @PostMapping("/{jobId}/approval")
    public ResponseEntity<?> approve(@PathVariable UUID jobId, @RequestBody ApprovalRequest request) {
        try {
            PipelineResponse response = orchestrator.approve(jobId, request.approved(), request.feedback());
            return ResponseEntity.ok(response);
        } catch (IllegalArgumentException e) {
            // e.g. rejected without feedback
            return ResponseEntity.badRequest().body(java.util.Map.of("error", e.getMessage()));
        } catch (IllegalStateException e) {
            // e.g. job is not currently awaiting approval
            return ResponseEntity.badRequest().body(java.util.Map.of("error", e.getMessage()));
        }
    }

    /**
     * "Approve all" - flip the job into auto-approve mode so every remaining agent runs
     * unattended to COMPLETED, and (if currently paused) advance the current stage now.
     * This is the single-action "skip the gate, keep going to the end".
     *
     * Usage: POST /api/pipeline/{jobId}/approve-all
     */
    @PostMapping("/{jobId}/approve-all")
    public ResponseEntity<PipelineResponse> approveAll(@PathVariable UUID jobId) {
        return ResponseEntity.ok(orchestrator.approveAll(jobId));
    }

    /**
     * Phase 9 manual test harness for the Architect Agent.
     * Reads the BA output for an existing job and runs the architect over it.
     *
     * Usage:
     *   GET /api/pipeline/test/architect?jobId=<existing-uuid>
     * where <existing-uuid> is a job that already has requirements.json
     * (i.e. the BA Agent has run for it).
     */
    @GetMapping("/test/architect")
    public ResponseEntity<String> testArchitect(@RequestParam String jobId) {
        String requirements = storageService.read("jobs/" + jobId + "/requirements/requirements.json");
        if (requirements == null || requirements.isBlank()) {
            return ResponseEntity.badRequest()
                    .body("No requirements.json found for jobId=" + jobId
                            + ". Run the BA Agent for this job first.");
        }
        String result = architectAgent.design(requirements, jobId);
        return ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(result);
    }

    /**
     * Phase 10 manual test harness for the Developer Agent.
     * Reads the architect's design for an existing job and runs the developer over it.
     *
     * Usage:
     *   GET /api/pipeline/test/developer?jobId=<existing-uuid>
     * where <existing-uuid> is a job that already has design artifacts
     * (i.e. the Architect Agent has run for it).
     */
    @GetMapping("/test/developer")
    public ResponseEntity<String> testDeveloper(@RequestParam String jobId) {
        String lld = storageService.read("jobs/" + jobId + "/design/lld.md");
        String schema = storageService.read("jobs/" + jobId + "/design/schema.sql");
        if ((lld == null || lld.isBlank()) && (schema == null || schema.isBlank())) {
            return ResponseEntity.badRequest()
                    .body("No design artifacts found for jobId=" + jobId
                            + ". Run the Architect Agent for this job first.");
        }
        String result = developerAgent.develop(null, jobId);
        return ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(result);
    }

    /**
     * Phase 11 manual test harness for the Tester Agent.
     * Reads the generated code for an existing job and runs the tester over it: generates a
     * test suite (factories + unit + integration) into src/test/java, injects test deps + an H2
     * test profile if missing, and runs a self-correcting `mvn test-compile` loop.
     *
     * Usage:
     *   GET /api/pipeline/test/tester?jobId=<existing-uuid>
     * where <existing-uuid> is a job that already has generated code
     * (i.e. the Developer Agent has run for it).
     */
    @GetMapping("/test/tester")
    public ResponseEntity<String> testTester(@RequestParam String jobId) {
        String pom = storageService.read("jobs/" + jobId + "/code/pom.xml");
        if (pom == null || pom.isBlank()) {
            return ResponseEntity.badRequest()
                    .body("No generated code found for jobId=" + jobId
                            + ". Run the Developer Agent for this job first.");
        }
        String result = testerAgent.test(null, jobId);
        return ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(result);
    }

    /**
     * Phase 12 manual test harness for the DevOps Agent.
     * Reads the generated code for an existing job and runs the DevOps agent over it: generates
     * Dockerfile (multi-stage Maven+JDK 21), docker-compose.yml (app+postgres), and a GitHub
     * Actions CI/CD workflow, saving them to BOTH jobs/{jobId}/code/ (runnable in place) and
     * jobs/{jobId}/devops/ (traceable copy).
     *
     * Usage:
     *   GET /api/pipeline/test/devops?jobId=<existing-uuid>
     * where <existing-uuid> is a job that already has generated code
     * (i.e. the Developer Agent has run for it).
     */
    @GetMapping("/test/devops")
    public ResponseEntity<String> testDevOps(@RequestParam String jobId) {
        String pom = storageService.read("jobs/" + jobId + "/code/pom.xml");
        if (pom == null || pom.isBlank()) {
            return ResponseEntity.badRequest()
                    .body("No generated code found for jobId=" + jobId
                            + ". Run the Developer Agent for this job first.");
        }
        String result = devOpsAgent.deploy(null, jobId);
        return ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(result);
    }
}
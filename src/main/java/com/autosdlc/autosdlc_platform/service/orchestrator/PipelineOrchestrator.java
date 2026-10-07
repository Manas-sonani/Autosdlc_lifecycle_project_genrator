package com.autosdlc.autosdlc_platform.service.orchestrator;

import com.autosdlc.autosdlc_platform.config.RabbitMQConfig;
import com.autosdlc.autosdlc_platform.model.dto.*;
import com.autosdlc.autosdlc_platform.model.entities.PipelineJob;
import com.autosdlc.autosdlc_platform.model.entities.AgentTask;
import com.autosdlc.autosdlc_platform.model.enums.AgentType;
import com.autosdlc.autosdlc_platform.model.enums.PipelineStatus;
import com.autosdlc.autosdlc_platform.model.repository.PipelineJobRepository;
import com.autosdlc.autosdlc_platform.model.repository.AgentTaskRepository;
import com.autosdlc.autosdlc_platform.service.storage.ArtifactStorageService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;
import java.time.LocalDateTime;
import java.util.UUID;

@Service @RequiredArgsConstructor @Slf4j
public class PipelineOrchestrator {
    private final PipelineJobRepository jobRepository;
    private final AgentTaskRepository taskRepository;
    private final RabbitTemplate rabbitTemplate;
    private final ArtifactStorageService storageService;

    public PipelineResponse startPipeline(StartPipelineRequest request) {
        var job = PipelineJob.builder()
                .userRequest(request.getDescription())
                .status(PipelineStatus.PENDING)
                .currentStage("BA_PENDING")
                .createdAt(LocalDateTime.now())
                .autoApprove(request.isAutoApprove())
                .build();
        job = jobRepository.save(job);

        var task = AgentTask.builder()
                .jobId(job.getId())
                .agentType(AgentType.BUSINESS_ANALYST)
                .taskType("ANALYZE")
                .inputData(request.getDescription())
                .status(PipelineStatus.PENDING)
                .startedAt(LocalDateTime.now())
                .build();
        task = taskRepository.save(task);

        rabbitTemplate.convertAndSend(RabbitMQConfig.EXCHANGE, RabbitMQConfig.BA_KEY,
                new BaTaskMessage(job.getId().toString(), request.getDescription(), task.getId()));
        log.info("Published to ba queue jobId={}", job.getId());

        job.setStatus(PipelineStatus.RUNNING);
        jobRepository.save(job);

        return PipelineResponse.builder().jobId(job.getId()).status(PipelineStatus.RUNNING).message("Queued").build();
    }
    public PipelineResponse getStatus(UUID jobId) {
        PipelineJob job = jobRepository.findById(jobId)
                .orElseThrow(() -> new RuntimeException("Job not found: " + jobId));

        return PipelineResponse.builder()
                .jobId(job.getId())
                .status(job.getStatus())
                .currentStage(job.getCurrentStage())
                .message(job.getErrorMessage())
                .createdAt(job.getCreatedAt())
                .build();
    }

    // ---- agent-done handlers: GATE for human approval ----------------------
    // Each agent finishes -> if the job is in auto-approve mode, auto-advance to the next agent
    // (the original end-to-end behavior); otherwise pause in AWAITING_APPROVAL for the reviewer
    // to call approve(jobId, ...) or approveAll(jobId). DevOps pauses in its own listener.

    @RabbitListener(queues = RabbitMQConfig.ORCH_QUEUE)
    public void onBaDone(BaDoneMessage msg) {
        var job = jobRepository.findById(UUID.fromString(msg.jobId())).orElseThrow();
        if (job.isAutoApprove()) {
            advance(job, "ARCHITECT_PENDING", RabbitMQConfig.ARCH_KEY, new ArchitectTaskMessage(msg.jobId()));
        } else {
            job.setStatus(PipelineStatus.AWAITING_APPROVAL);
            job.setCurrentStage("BA_AWAITING_APPROVAL");
            jobRepository.save(job);
            log.info("BA done -> awaiting human approval for jobId={}", msg.jobId());
        }
    }

    @RabbitListener(queues = RabbitMQConfig.ARCH_DONE_QUEUE)
    public void onArchitectDone(ArchitectDoneMessage msg) {
        var job = jobRepository.findById(UUID.fromString(msg.jobId())).orElseThrow();
        if (job.isAutoApprove()) {
            advance(job, "DEVELOPER_PENDING", RabbitMQConfig.DEV_KEY, new DeveloperTaskMessage(msg.jobId()));
        } else {
            job.setStatus(PipelineStatus.AWAITING_APPROVAL);
            job.setCurrentStage("ARCHITECT_AWAITING_APPROVAL");
            jobRepository.save(job);
            log.info("Architect done -> awaiting human approval for jobId={}", msg.jobId());
        }
    }

    @RabbitListener(queues = RabbitMQConfig.DEV_DONE_QUEUE)
    public void onDeveloperDone(DeveloperDoneMessage msg) {
        var job = jobRepository.findById(UUID.fromString(msg.jobId())).orElseThrow();
        if (job.isAutoApprove()) {
            advance(job, "TESTER_PENDING", RabbitMQConfig.TEST_KEY, new TesterTaskMessage(msg.jobId()));
        } else {
            job.setStatus(PipelineStatus.AWAITING_APPROVAL);
            job.setCurrentStage("DEVELOPER_AWAITING_APPROVAL");
            jobRepository.save(job);
            log.info("Developer done -> awaiting human approval for jobId={}", msg.jobId());
        }
    }

    @RabbitListener(queues = RabbitMQConfig.TEST_DONE_QUEUE)
    public void onTesterDone(TesterDoneMessage msg) {
        var job = jobRepository.findById(UUID.fromString(msg.jobId())).orElseThrow();
        if (job.isAutoApprove()) {
            advance(job, "DEVOPS_PENDING", RabbitMQConfig.DEVOPS_KEY, new DevOpsTaskMessage(msg.jobId()));
        } else {
            job.setStatus(PipelineStatus.AWAITING_APPROVAL);
            job.setCurrentStage("TESTER_AWAITING_APPROVAL");
            jobRepository.save(job);
            log.info("Tester done -> awaiting human approval for jobId={}", msg.jobId());
        }
    }

    // ---- human approval gate ------------------------------------------------

    /**
     * Handle a reviewer's approval decision for a paused job.
     *
     * approved=true  -> advance to the next agent (publish its task message), or mark the
     *                   pipeline COMPLETED if DevOps was the one awaiting approval. Clears any
     *                   feedback for the current agent.
     * approved=false -> requires non-blank feedback. Saves it (jobs/{jobId}/feedback/<agent>.txt)
     *                   and re-triggers the SAME agent so it regenerates with the feedback
     *                   injected into its LLM prompt, then pauses for approval again.
     *
     * Returns a PipelineResponse describing what happened. Throws if the job isn't currently
     * awaiting approval, or if a reject has no feedback.
     */
    public PipelineResponse approve(UUID jobId, boolean approved, String feedback) {
        PipelineJob job = jobRepository.findById(jobId)
                .orElseThrow(() -> new RuntimeException("Job not found: " + jobId));
        String stage = job.getCurrentStage() == null ? "" : job.getCurrentStage();

        if (approved) {
            return handleApprove(job, stage);
        }
        return handleReject(job, stage, feedback);
    }

    /**
     * "Approve all": flip the job into auto-approve mode so every remaining agent runs
     * unattended to COMPLETED, AND - if the job is currently paused at a stage - advance that
     * stage now. If the job is mid-run (an agent is working), just set the flag; the next
     * done-handler will auto-advance. This is the single-action "skip the gate, keep going".
     */
    public PipelineResponse approveAll(UUID jobId) {
        PipelineJob job = jobRepository.findById(jobId)
                .orElseThrow(() -> new RuntimeException("Job not found: " + jobId));
        job.setAutoApprove(true);
        jobRepository.save(job);
        log.info("Auto-approve ENABLED for jobId={}", jobId);

        String stage = job.getCurrentStage() == null ? "" : job.getCurrentStage();
        if (stage.contains("AWAITING")) {
            // Currently paused -> advance this stage now; subsequent stages auto-advance.
            return handleApprove(job, stage);
        }
        // Mid-run -> flag is set; the in-flight agent's done-handler will auto-advance.
        return response(job, "Auto-approve enabled; remaining stages will run unattended");
    }

    /** Approve: advance to the next stage (or complete after DevOps). */
    private PipelineResponse handleApprove(PipelineJob job, String stage) {
        String jobId = job.getId().toString();

        if (stage.contains("BA_AWAITING")) {
            storageService.clearFeedback(jobId, "ba");
            advance(job, "ARCHITECT_PENDING", RabbitMQConfig.ARCH_KEY, new ArchitectTaskMessage(jobId));
            return response(job, "Approved BA -> Architect running");
        }
        if (stage.contains("ARCHITECT_AWAITING")) {
            storageService.clearFeedback(jobId, "architect");
            advance(job, "DEVELOPER_PENDING", RabbitMQConfig.DEV_KEY, new DeveloperTaskMessage(jobId));
            return response(job, "Approved Architect -> Developer running");
        }
        if (stage.contains("DEVELOPER_AWAITING")) {
            storageService.clearFeedback(jobId, "developer");
            advance(job, "TESTER_PENDING", RabbitMQConfig.TEST_KEY, new TesterTaskMessage(jobId));
            return response(job, "Approved Developer -> Tester running");
        }
        if (stage.contains("TESTER_AWAITING")) {
            storageService.clearFeedback(jobId, "tester");
            advance(job, "DEVOPS_PENDING", RabbitMQConfig.DEVOPS_KEY, new DevOpsTaskMessage(jobId));
            return response(job, "Approved Tester -> DevOps running");
        }
        if (stage.contains("DEVOPS_AWAITING")) {
            storageService.clearFeedback(jobId, "devops");
            job.setStatus(PipelineStatus.COMPLETED);
            job.setCurrentStage("DEVOPS_COMPLETED");
            job.setCompletedAt(LocalDateTime.now());
            jobRepository.save(job);
            log.info("Approved DevOps -> pipeline COMPLETED for jobId={}", jobId);
            return response(job, "Approved DevOps -> pipeline COMPLETED");
        }
        throw new IllegalStateException(
                "Job " + job.getId() + " is not awaiting approval (currentStage=" + stage + ")");
    }

    /** Reject: save feedback and re-run the current agent so it regenerates with the feedback. */
    private PipelineResponse handleReject(PipelineJob job, String stage, String feedback) {
        if (feedback == null || feedback.isBlank()) {
            throw new IllegalArgumentException("Feedback is required when rejecting an agent's output");
        }
        String jobId = job.getId().toString();

        if (stage.contains("BA_AWAITING")) {
            storageService.saveFeedback(jobId, "ba", feedback);
            // BA needs a BaTaskMessage(jobId, userRequest, taskId) - create a fresh AgentTask for the revise.
            var task = AgentTask.builder()
                    .jobId(job.getId())
                    .agentType(AgentType.BUSINESS_ANALYST)
                    .taskType("REVISE")
                    .inputData(feedback)
                    .status(PipelineStatus.PENDING)
                    .startedAt(LocalDateTime.now())
                    .build();
            task = taskRepository.save(task);
            revise(job, "BA_REVISING", RabbitMQConfig.BA_KEY,
                    new BaTaskMessage(jobId, job.getUserRequest(), task.getId()));
            return response(job, "Rejected BA -> re-running with feedback");
        }
        if (stage.contains("ARCHITECT_AWAITING")) {
            storageService.saveFeedback(jobId, "architect", feedback);
            revise(job, "ARCHITECT_REVISING", RabbitMQConfig.ARCH_KEY, new ArchitectTaskMessage(jobId));
            return response(job, "Rejected Architect -> re-running with feedback");
        }
        if (stage.contains("DEVELOPER_AWAITING")) {
            storageService.saveFeedback(jobId, "developer", feedback);
            revise(job, "DEVELOPER_REVISING", RabbitMQConfig.DEV_KEY, new DeveloperTaskMessage(jobId));
            return response(job, "Rejected Developer -> re-running with feedback");
        }
        if (stage.contains("TESTER_AWAITING")) {
            storageService.saveFeedback(jobId, "tester", feedback);
            revise(job, "TESTER_REVISING", RabbitMQConfig.TEST_KEY, new TesterTaskMessage(jobId));
            return response(job, "Rejected Tester -> re-running with feedback");
        }
        if (stage.contains("DEVOPS_AWAITING")) {
            storageService.saveFeedback(jobId, "devops", feedback);
            revise(job, "DEVOPS_REVISING", RabbitMQConfig.DEVOPS_KEY, new DevOpsTaskMessage(jobId));
            return response(job, "Rejected DevOps -> re-running with feedback");
        }
        throw new IllegalStateException(
                "Job " + job.getId() + " is not awaiting approval (currentStage=" + stage + ")");
    }

    /** Set RUNNING + next stage and publish the next agent's task message. */
    private void advance(PipelineJob job, String nextStage, String routingKey, Object taskMessage) {
        job.setStatus(PipelineStatus.RUNNING);
        job.setCurrentStage(nextStage);
        jobRepository.save(job);
        rabbitTemplate.convertAndSend(RabbitMQConfig.EXCHANGE, routingKey, taskMessage);
        log.info("Approved -> {} for jobId={}", nextStage, job.getId());
    }

    /** Set RUNNING + <agent>_REVISING and re-publish the current agent's task message. */
    private void revise(PipelineJob job, String revisingStage, String routingKey, Object taskMessage) {
        job.setStatus(PipelineStatus.RUNNING);
        job.setCurrentStage(revisingStage);
        jobRepository.save(job);
        rabbitTemplate.convertAndSend(RabbitMQConfig.EXCHANGE, routingKey, taskMessage);
        log.info("Rejected -> {} (re-run with feedback) for jobId={}", revisingStage, job.getId());
    }

    private PipelineResponse response(PipelineJob job, String message) {
        return PipelineResponse.builder()
                .jobId(job.getId())
                .status(job.getStatus())
                .currentStage(job.getCurrentStage())
                .message(message)
                .createdAt(job.getCreatedAt())
                .build();
    }
}


//package com.autosdlc.autosdlc_platform.service.orchestrator;
//
//
//import com.autosdlc.autosdlc_platform.model.dto.PipelineResponse;
//import com.autosdlc.autosdlc_platform.model.dto.StartPipelineRequest;
//import com.autosdlc.autosdlc_platform.model.entities.PipelineJob;
//import com.autosdlc.autosdlc_platform.model.enums.PipelineStatus;
//import com.autosdlc.autosdlc_platform.model.repository.PipelineJobRepository;
//import com.autosdlc.autosdlc_platform.service.agents.BusinessAnalystAgent;
//import lombok.RequiredArgsConstructor;
//import lombok.extern.slf4j.Slf4j;
//import org.springframework.stereotype.Service;
//import org.springframework.transaction.annotation.Transactional;
//
//import java.time.LocalDateTime;
//import java.util.UUID;
//
//@Service
//@RequiredArgsConstructor
//@Slf4j
//public class PipelineOrchestrator {
//
//    private final PipelineJobRepository jobRepository;
//    private final BusinessAnalystAgent businessAnalystAgent;
//
//    @Transactional
//    public PipelineResponse startPipeline(StartPipelineRequest request) {
//        // 1. Create job
//        PipelineJob job = PipelineJob.builder()
//                .userRequest(request.getDescription())
//                .status(PipelineStatus.RUNNING)
//                .currentStage("BA_ANALYSIS")
//                .createdAt(LocalDateTime.now())
//                .build();
//        job = jobRepository.save(job);
//        log.info("Pipeline started: jobId={} project={}", job.getId(), request.getProjectName());
//
//        try {
//            // 2. Call BA Agent - SYNCHRONOUS for now
//            job.setStatus(PipelineStatus.RUNNING);
//            jobRepository.save(job);
//
//            businessAnalystAgent.analyze(request.getDescription(), job.getId().toString());
//
//            // 3. Mark completed
//            job.setStatus(PipelineStatus.COMPLETED);
//            job.setCurrentStage("BA_COMPLETED");
//            job.setCompletedAt(LocalDateTime.now());
//            jobRepository.save(job);
//
//            return PipelineResponse.builder()
//                    .jobId(job.getId())
//                    .projectName(request.getProjectName())
//                    .status(job.getStatus())
//                    .currentStage(job.getCurrentStage())
//                    .message("Pipeline completed successfully")
//                    .createdAt(job.getCreatedAt())
//                    .build();
//
//        } catch (Exception e) {
//            log.error("Pipeline failed for jobId={}", job.getId(), e);
//            job.setStatus(PipelineStatus.FAILED);
//            job.setCurrentStage("FAILED_AT_BA");
//            job.setErrorMessage(e.getMessage());
//            job.setCompletedAt(LocalDateTime.now());
//            jobRepository.save(job);
//
//            return PipelineResponse.builder()
//                    .jobId(job.getId())
//                    .projectName(request.getProjectName())
//                    .status(PipelineStatus.FAILED)
//                    .message("Pipeline failed: " + e.getMessage())
//                    .build();
//        }
//    }
//
//    public PipelineResponse getStatus(UUID jobId) {
//        PipelineJob job = jobRepository.findById(jobId)
//                .orElseThrow(() -> new RuntimeException("Job not found: " + jobId));
//
//        return PipelineResponse.builder()
//                .jobId(job.getId())
//                .status(job.getStatus())
//                .currentStage(job.getCurrentStage())
//                .message(job.getErrorMessage())
//                .createdAt(job.getCreatedAt())
//                .build();
//    }
//}
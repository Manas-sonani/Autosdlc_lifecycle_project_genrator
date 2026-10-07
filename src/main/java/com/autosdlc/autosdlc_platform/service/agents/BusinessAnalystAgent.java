package com.autosdlc.autosdlc_platform.service.agents;


import com.autosdlc.autosdlc_platform.config.RabbitMQConfig;
import com.autosdlc.autosdlc_platform.model.dto.BaDoneMessage;
import com.autosdlc.autosdlc_platform.model.dto.BaTaskMessage;
import com.autosdlc.autosdlc_platform.model.dto.ba.RequirementsResponse;
import com.autosdlc.autosdlc_platform.model.entities.Artifact;
import com.autosdlc.autosdlc_platform.model.entities.PipelineJob;
import com.autosdlc.autosdlc_platform.model.enums.AgentType;
import com.autosdlc.autosdlc_platform.model.enums.ArtifactType;
import com.autosdlc.autosdlc_platform.model.enums.PipelineStatus;
import com.autosdlc.autosdlc_platform.model.repository.ArtifactRepository;
import com.autosdlc.autosdlc_platform.model.repository.PipelineJobRepository;
import com.autosdlc.autosdlc_platform.service.knowledgebase.KnowledgeBaseService;
import com.autosdlc.autosdlc_platform.service.llm.LLMClient;
import com.autosdlc.autosdlc_platform.service.storage.ArtifactStorageService;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import tools.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.time.LocalDateTime;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class BusinessAnalystAgent {

    private final KnowledgeBaseService knowledgeBaseService;
    private final LLMClient llmClient;
    private final ArtifactStorageService storageService;
    private final ArtifactRepository artifactRepository;
    private final PipelineJobRepository pipelineJobRepository;
    private final ObjectMapper objectMapper;
    private final RabbitTemplate rabbitTemplate;


//    @RabbitListener(queues = RabbitMQConfig.BA_QUEUE)
//    public void onBaTask(BaTaskMessage message) {
//        log.info("BA received jobId={}", message.jobId());
//        analyze(message.userRequest(), message.jobId()); // your existing method
//        rabbitTemplate.convertAndSend(RabbitMQConfig.EXCHANGE, RabbitMQConfig.ORCH_KEY,
//                new BaDoneMessage(message.jobId(), "COMPLETED"));
//    }
@RabbitListener(queues = RabbitMQConfig.BA_QUEUE)
public void onBaTask(BaTaskMessage message) {
    log.info("BA received jobId={}", message.jobId());
    try {
        analyze(message.userRequest(), message.jobId());
        rabbitTemplate.convertAndSend(RabbitMQConfig.EXCHANGE, RabbitMQConfig.ORCH_KEY,
                new BaDoneMessage(message.jobId(), "COMPLETED"));
        log.info("BA completed jobId={}", message.jobId());
    } catch (Exception e) {
        log.error("BA failed for jobId={} - marking FAILED, not requeue", message.jobId(), e);
        // update job to FAILED so you can see it
        try {
            var job = pipelineJobRepository.findById(UUID.fromString(message.jobId())).orElse(null);
            if (job != null) {
                job.setStatus(PipelineStatus.FAILED);
                job.setCurrentStage("FAILED_AT_BA: " + e.getMessage());
                pipelineJobRepository.save(job);
            }
        } catch (Exception ex) {}
        // DO NOT THROW - if you throw, RabbitMQ will loop forever
    }
}


    @Transactional
    public RequirementsResponse analyze(String userRequest, String jobId) {
        log.info("BA Agent started for jobId={}", jobId);

        // Step 1 & 2: Query KB (trimmed hard for token budget during testing)
        String standards = knowledgeBaseService.getContextForAgent(AgentType.BUSINESS_ANALYST, userRequest);
        if (standards.length() > 800) {
            standards = standards.substring(0, 800);
        }
        log.info("KB context loaded: {} chars", standards.length());



        // Step 3: Build Prompt
        // MINIMAL-FOR-TESTING: request the smallest valid payload so it fits well under
        // the Groq max_tokens cap and costs the least. Bump counts back up once on a
        // higher tier / when no longer testing.
        String systemPrompt = """
            You are a Senior Business Analyst at AutoSDLC Factory.
            Standards: %s
            RULES:
            - Output ONLY valid JSON, no markdown, no ```json wrapper.
            - Every requirement has an id like REQ-001.
            - Keep each field to ONE short sentence.
            """.formatted(standards);

        String userPrompt = """
            Idea: "%s"
            Output ONLY this JSON (fill values, keep everything short):
            {
              "projectName": "short name",
              "projectDescription": "one sentence",
              "functionalRequirements": [
                {"id":"REQ-001","title":"...","description":"...","priority":"MUST","acceptanceCriteria":"Given... When... Then..."},
                {"id":"REQ-002","title":"...","description":"...","priority":"SHOULD","acceptanceCriteria":"Given... When... Then..."}
              ],
              "nonFunctionalRequirements": [
                {"id":"NFR-001","category":"security","description":"..."}
              ],
              "userStories": [
                {"id":"US-001","role":"user","feature":"I want to...","benefit":"so that...","acceptanceCriteria":["..."]},
                {"id":"US-002","role":"user","feature":"I want to...","benefit":"so that...","acceptanceCriteria":["..."]}
              ],
              "useCases": [
                {"id":"UC-001","actor":"User","title":"...","steps":["Step 1","Step 2"]}
              ]
            }
            Exactly 2 functional, 1 non-functional, 2 user stories, 1 use case. Nothing else.
            """.formatted(userRequest);

        // Human approval gate: if the reviewer rejected a previous BA run with feedback,
        // incorporate those modification notes into this re-run.
        String feedback = storageService.readFeedback(jobId, "ba");
        if (feedback != null) {
            userPrompt += "\n\nREVIEWER FEEDBACK from the previous run — incorporate these changes " +
                    "(regenerate the full JSON with the feedback applied):\n" +
                    (feedback.length() > 1500 ? feedback.substring(0, 1500) : feedback);
            log.info("BA re-run with reviewer feedback ({} chars)", feedback.length());
        }

        // Step 4: Call LLM via the BA provider chain (auto-fallback on 429/quota).
        // 1500 cap is plenty for the minimal payload above; well under Groq limit.
        String rawResponse = llmClient.chat(AgentType.BUSINESS_ANALYST, systemPrompt, userPrompt, 1500);
        String cleanJson = extractJson(rawResponse);
        log.info("LLM raw response cleaned");

        try {
            // Step 5: Parse and Save
            RequirementsResponse parsed = objectMapper.readValue(cleanJson, RequirementsResponse.class);

            String minioPath = storageService.saveJson(jobId, "requirements", "requirements.json", cleanJson);

            // Step 6: Create Artifact Record
            PipelineJob job = pipelineJobRepository.findById(UUID.fromString(jobId)).orElse(null);

            Artifact artifact = Artifact.builder()
                    .pipelineJob(job)
                    .artifactType(ArtifactType.REQUIREMENTS)
                    .fileName("requirements.json")
                    .minioPath(minioPath)
                    .contentType("application/json")
                    .fileSize((long) cleanJson.length())
                    .createdAt(LocalDateTime.now())
                    .build();
            artifactRepository.save(artifact);

            if(job != null){
                job.setCurrentStage("BA_COMPLETED");
                job.setStatus(PipelineStatus.RUNNING);
                pipelineJobRepository.save(job);
            }

            log.info("BA Agent COMPLETED for jobId={}", jobId);
            return parsed;

        } catch (Exception e) {
            log.error("Failed to parse BA response. raw length={}, clean length={}. cleanJson:\n{}",
                    rawResponse == null ? 0 : rawResponse.length(),
                    cleanJson == null ? 0 : cleanJson.length(),
                    cleanJson, e);
            throw new RuntimeException("BA Agent failed to parse LLM JSON: " + cleanJson, e);
        }
    }

    private String extractJson(String text) {
        // Removes ```json ... ``` if LLM adds it
        text = text.trim();
        if (text.startsWith("```")) {
            text = text.replaceAll("(?s)```json|```", "").trim();
        }
        int start = text.indexOf('{');
        int end = text.lastIndexOf('}');
        if (start != -1 && end != -1) {
            return text.substring(start, end + 1);
        }
        System.out.println("this is text ............... = " +text);
        return text;
    }
}
package com.autosdlc.autosdlc_platform.service.agents;

import com.autosdlc.autosdlc_platform.config.RabbitMQConfig;
import com.autosdlc.autosdlc_platform.model.dto.ArchitectDoneMessage;
import com.autosdlc.autosdlc_platform.model.dto.ArchitectTaskMessage;
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
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Architect Agent - Phase 9.
 *
 * Takes the BA Agent's requirements.json and produces the HOW:
 *   - HLD (high level design) with a Mermaid component diagram   -> hld.md
 *   - LLD (low level design) with class + sequence diagrams       -> lld.md
 *   - PostgreSQL DDL                                              -> schema.sql
 *   - OpenAPI 3.0 spec                                            -> api-spec.yaml
 *   - HTML wireframes                                             -> wireframes.html
 *
 * Follows the same 6-step pattern as BusinessAnalystAgent. The key difference:
 * this agent READS the previous agent's output from storage before working.
 *
 * IMPORTANT - one LLM call PER artifact (not one giant call):
 * Producing all 5 docs in one call risks truncation and gives the model less focus,
 * so we make 5 small, focused calls - each returns one clean artifact as raw content.
 *
 * This agent calls the LLM via the DESIGNER provider chain (see llm.agents.DESIGNER in
 * application.properties), which auto-falls back across providers on 429/quota. The chain is
 * Groq (gpt-oss-120b, fast) first with local Ollama as a last-resort fallback - local was too
 * slow for 5 sequential calls. Output is kept minimal (concise prompts, tight max_tokens, trimmed
 * KB context) to stay under Groq's free-tier tokens-per-minute cap. Add more Groq keys under
 * llm.providers.* and put them in the chain to multiply quota without code changes.
 *
 * Not yet wired to a RabbitMQ queue - in Phase 9 it is invoked via the
 * /api/pipeline/test/architect endpoint. Queue wiring + approval flow come later.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ArchitectAgent {

    private final KnowledgeBaseService knowledgeBaseService;
    private final ArtifactStorageService storageService;
    private final LLMClient llmClient;
    private final ArtifactRepository artifactRepository;
    private final PipelineJobRepository pipelineJobRepository;
    private final RabbitTemplate rabbitTemplate;

    /**
     * Auto-triggered when BA completes: the orchestrator publishes an ArchitectTaskMessage
     * to the architect queue, and this listener runs the design phase. requirementsJson is
     * left null so design() reads BA's requirements.json from storage by jobId.
     *
     * On success the job is marked RUNNING at ARCHITECT_COMPLETED and an ArchitectDoneMessage
     * is published to the architect-done queue so the orchestrator can hand off to the
     * Developer Agent (mirrors how BA publishes BaDoneMessage after finishing). On failure
     * the job is marked FAILED and NOT requeued (see setDefaultRequeueRejected(false)).
     */
    @RabbitListener(queues = RabbitMQConfig.ARCH_QUEUE)
    public void onArchitectTask(ArchitectTaskMessage message) {
        log.info("Architect received jobId={}", message.jobId());
        try {
            design(null, message.jobId());
            var job = pipelineJobRepository.findById(UUID.fromString(message.jobId())).orElse(null);
            if (job != null) {
                job.setStatus(PipelineStatus.RUNNING);
                job.setCurrentStage("ARCHITECT_COMPLETED");
                pipelineJobRepository.save(job);
            }
            rabbitTemplate.convertAndSend(RabbitMQConfig.EXCHANGE, RabbitMQConfig.ARCH_DONE_KEY,
                    new ArchitectDoneMessage(message.jobId()));
            log.info("Architect completed jobId={} -> published ArchitectDoneMessage", message.jobId());
        } catch (Exception e) {
            log.error("Architect failed for jobId={} - marking FAILED, not requeue", message.jobId(), e);
            try {
                var job = pipelineJobRepository.findById(UUID.fromString(message.jobId())).orElse(null);
                if (job != null) {
                    job.setStatus(PipelineStatus.FAILED);
                    job.setCurrentStage("FAILED_AT_ARCHITECT: " + e.getMessage());
                    job.setCompletedAt(LocalDateTime.now());
                    pipelineJobRepository.save(job);
                }
            } catch (Exception ignored) {}
            // Do NOT throw - throwing would make RabbitMQ requeue/loop forever.
        }
    }

    /** One spec per design artifact. maxTokens tuned per artifact size. */
    private record ArtifactSpec(String key, String fileName, ArtifactType type,
                                String contentType, int maxTokens, boolean stripFence,
                                String instruction) {}

    private static final List<ArtifactSpec> SPECS = List.of(
            new ArtifactSpec("hld", "hld.md", ArtifactType.DESIGN_HLD, "text/markdown", 1000, false,
                    "High Level Design: concise markdown with a Mermaid component diagram " +
                    "(```mermaid graph TD) showing the main components and interactions. " +
                    "Keep it short - 1 tech-stack line, no essays."),
            new ArtifactSpec("lld", "lld.md", ArtifactType.DESIGN_LLD, "text/markdown", 1000, false,
                    "Low Level Design: concise markdown listing the main classes and key methods, " +
                    "plus one Mermaid class diagram for the core flow. Keep it short."),
            new ArtifactSpec("databaseSchema", "schema.sql", ArtifactType.DESIGN_SCHEMA,
                    "application/sql", 500, true,
                    "PostgreSQL DDL: CREATE TABLE statements with primary keys and foreign keys only. " +
                    "Minimal tables. Output raw SQL only."),
            new ArtifactSpec("apiSpec", "api-spec.yaml", ArtifactType.DESIGN_API_SPEC,
                    "application/yaml", 800, true,
                    "OpenAPI 3.0 YAML spec for the REST API. Must start with 'openapi: 3.0.0'. " +
                    "Only the core endpoints. Output raw YAML only."),
            new ArtifactSpec("wireframes", "wireframes.html", ArtifactType.DESIGN_WIREFRAME,
                    "text/html", 1200, true,
                    "HTML wireframes: a single HTML file using the Tailwind CDN, responsive, for " +
                    "the 2 main pages only. Output raw HTML only.")
    );

    /**
     * Pause between LLM calls. Currently unused - kept for when the Architect is switched
     * back to a rate-limited provider (e.g. Groq free tier caps at 8K tokens/minute). The
     * LOCAL Ollama provider has no rate limit, so no delay is applied between calls.
     */
    @SuppressWarnings("unused")
    private static final long DELAY_BETWEEN_CALLS_MS = 20_000L;

    // Step 1: Receive Task
    public String design(String requirementsJson, String jobId) {
        log.info("Architect Agent started for jobId: {}", jobId);
        try {
            // Step 2: Read BA Output from storage (the path BA wrote to)
            String baRequirements = requirementsJson;
            if (baRequirements == null || baRequirements.isBlank()) {
                String path = "jobs/" + jobId + "/requirements/requirements.json";
                log.info("Reading BA output from storage: {}", path);
                baRequirements = storageService.read(path);
                if (baRequirements == null || baRequirements.isBlank()) {
                    throw new RuntimeException(
                            "No requirements found for jobId " + jobId + " at " + path
                                    + ". Run the BA Agent for this job first.");
                }
            }

            // Human approval gate: if the reviewer rejected a previous Architect run with
            // feedback, append it to the requirements context so EVERY artifact call sees it.
            String feedback = storageService.readFeedback(jobId, "architect");
            if (feedback != null) {
                baRequirements += "\n\nREVIEWER FEEDBACK from the previous run — incorporate these " +
                        "changes into the design artifacts:\n" +
                        (feedback.length() > 1500 ? feedback.substring(0, 1500) : feedback);
                log.info("Architect re-run with reviewer feedback ({} chars)", feedback.length());
            }

            // Step 3: Query Knowledge Base (once - reused for every artifact call)
            log.info("Querying Knowledge Base for ARCHITECT context");
            String kbContext = knowledgeBaseService.getContextForAgent(AgentType.DESIGNER, baRequirements);
            // TOKEN-OPT: KB context is repeated in every per-artifact call, so trim it hard.
            if (kbContext.length() > 600) {
                kbContext = kbContext.substring(0, 600);
            }
            log.info("KB context loaded: {} chars", kbContext.length());

            // Step 4-6: For each artifact, build prompt, call LLM, parse (raw), save.
            List<String> saved = new ArrayList<>();
            for (int i = 0; i < SPECS.size(); i++) {
                ArtifactSpec spec = SPECS.get(i);
                String content = generateArtifact(spec, baRequirements, kbContext);
                String relativePath = "jobs/" + jobId + "/design/" + spec.fileName();
                String storedPath = storageService.save(relativePath, content);
                saveArtifactRow(jobId, spec, content, storedPath);
                saved.add(spec.fileName() + " (" + content.length() + " chars)");
                log.info("Saved artifact {} at {}", spec.type(), storedPath);

                // No artificial delay between calls: the Architect now runs on the LOCAL
                // Ollama provider, which has no rate limit. The model's own generation time
                // is the pacing. (The old 20s sleep was only needed for Groq's free-tier cap.)
            }

            String summary = "Architect completed. Saved: " + String.join(", ", saved);
            log.info(summary + " for jobId: {}", jobId);
            return summary;

        } catch (Exception e) {
            log.error("Architect Agent failed for jobId: {}", jobId, e);
            throw new RuntimeException("Architect Agent failed: " + e.getMessage(), e);
        }
    }

    /** Build a focused prompt for one artifact and call the LLM. Returns raw content. */
    private String generateArtifact(ArtifactSpec spec, String requirements, String kbContext) {
        String systemPrompt = """
                You are a Senior Solution Architect. Standards: %s
                Tech stack: Java 21, Spring Boot, PostgreSQL. Output ONLY the requested artifact,
                no preamble. Be concise. Do NOT wrap in a code fence unless part of the content.
                """.formatted(kbContext);

        String userPrompt = """
                Requirements: %s
                Generate this artifact (keep it short): %s
                """.formatted(requirements, spec.instruction());

        log.info("Calling LLM for {} via DESIGNER chain (max_tokens={})", spec.key(), spec.maxTokens());
        String raw = llmClient.chat(AgentType.DESIGNER, systemPrompt, userPrompt, spec.maxTokens());
        if (raw == null) {
            raw = "";
        }
        log.info("LLM response for {}: length={}", spec.key(), raw.length());

        return spec.stripFence() ? stripFence(raw) : raw.trim();
    }

    private void saveArtifactRow(String jobId, ArtifactSpec spec, String content, String storedPath) {
        PipelineJob job = pipelineJobRepository.findById(UUID.fromString(jobId)).orElse(null);

        Artifact artifact = Artifact.builder()
                .pipelineJob(job)
                .artifactType(spec.type())
                .fileName(spec.fileName())
                .minioPath(storedPath)
                .contentType(spec.contentType())
                .fileSize((long) content.length())
                .createdAt(LocalDateTime.now())
                .build();
        artifactRepository.save(artifact);
    }

    /**
     * Remove a single outer code fence (e.g. ```sql ... ``` or ```yaml ... ```).
     * Used for raw SQL / YAML / HTML artifacts where the LLM sometimes wraps output.
     * NOT used for markdown (hld/lld), which legitimately contain inner ```mermaid blocks.
     */
    private String stripFence(String text) {
        String t = text.trim();
        if (!t.startsWith("```")) {
            return t;
        }
        int firstNl = t.indexOf('\n');
        if (firstNl == -1) {
            return t;
        }
        t = t.substring(firstNl + 1);
        int lastFence = t.lastIndexOf("```");
        if (lastFence > -1) {
            t = t.substring(0, lastFence);
        }
        return t.trim();
    }
}
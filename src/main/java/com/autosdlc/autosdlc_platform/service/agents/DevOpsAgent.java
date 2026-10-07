package com.autosdlc.autosdlc_platform.service.agents;

import com.autosdlc.autosdlc_platform.config.RabbitMQConfig;
import com.autosdlc.autosdlc_platform.model.dto.DevOpsTaskMessage;
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
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * DevOps Agent - Phase 12.
 *
 * Takes the generated Spring Boot project (jobs/{jobId}/code/) + design and generates deployment
 * configs (Docker + CI only - Terraform/AWS was deliberately skipped per project decision):
 *   - Dockerfile (multi-stage: maven:3.9-eclipse-temurin-21 build -> JRE 21 runtime)
 *   - docker-compose.yml (app service + postgres service, app wired to the postgres service host)
 *   - .github/workflows/ci-cd.yml (on: push; build / test / deploy jobs)
 *
 * Context it reads: pom.xml (deps -> Spring Boot + PostgreSQL), application.yml (datasource URL,
 * user/pass, server port - so compose matches), schema.sql (tables the DB must provision). The
 * prompts feed the ACTUAL values so the generated Dockerfile/compose line up with the app (right
 * port, right DB name/user/pass, app connects to the `postgres` service hostname, not localhost).
 *
 * Output location: BOTH places (per project decision) -
 *   - jobs/{jobId}/code/<file>   so `docker build` / `docker compose up` / CI work IN PLACE, and
 *   - jobs/{jobId}/devops/<file> as a traceable canonical copy.
 * One Artifact row (DEPLOYMENT) per file, minioPath = the devops/ copy.
 *
 * One LLM call (the three files are short configs and are tightly coupled - compose builds from
 * the Dockerfile, CI builds/tests the same - so generating them together keeps them consistent).
 * No self-correcting compile loop: unlike Java, there is nothing to compile to verify a Dockerfile
 * or a compose file without Docker/Terraform installed. A later phase can add a `docker build`
 * smoke test if Docker is available locally.
 *
 * Wiring: the Tester Agent publishes a TesterDoneMessage when it finishes; the orchestrator
 * advances to DEVOPS_PENDING and publishes a DevOpsTaskMessage to the devops queue; this listener
 * runs the deploy phase. DevOps is the FINAL stage, so on success the job is marked COMPLETED
 * (DEVOPS_COMPLETED) - the whole pipeline BA -> Architect -> Developer -> Tester -> DevOps is done.
 * On failure the job is marked FAILED and NOT requeued (RabbitMQConfig.setDefaultRequeueRejected).
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class DevOpsAgent {

    private final KnowledgeBaseService knowledgeBaseService;
    private final ArtifactStorageService storageService;
    private final LLMClient llmClient;
    private final ArtifactRepository artifactRepository;
    private final PipelineJobRepository pipelineJobRepository;

    /** A single file parsed out of the LLM's multi-file response: its emitted path + raw content. */
    private record GeneratedFile(String path, String content) {}

    /** Header line the LLM uses to delimit each file, e.g. "### File: Dockerfile". */
    private static final Pattern FILE_HEADER =
            Pattern.compile("(?im)^###\\s*File:\\s*(.+?)\\s*$");

    /**
     * Auto-triggered when the Tester completes (and is approved): the orchestrator publishes a
     * DevOpsTaskMessage to the devops queue, and this listener runs the deploy phase. DevOps is
     * the final stage, so on success the job pauses for human approval (AWAITING_APPROVAL) -
     * the reviewer then approves to mark the pipeline COMPLETED, or rejects with feedback to
     * regenerate the deployment configs.
     */
    @RabbitListener(queues = RabbitMQConfig.DEVOPS_QUEUE)
    public void onDevOpsTask(DevOpsTaskMessage message) {
        log.info("DevOps received jobId={}", message.jobId());
        try {
            deploy(null, message.jobId());
            var job = pipelineJobRepository.findById(UUID.fromString(message.jobId())).orElse(null);
            if (job != null) {
                if (job.isAutoApprove()) {
                    // Auto-approve mode: the pipeline runs end-to-end, so complete now.
                    job.setStatus(PipelineStatus.COMPLETED);
                    job.setCurrentStage("DEVOPS_COMPLETED");
                    job.setCompletedAt(LocalDateTime.now());
                    pipelineJobRepository.save(job);
                    log.info("DevOps completed -> pipeline DONE (auto-approve) for jobId={}", message.jobId());
                } else {
                    // Pause for human approval - do NOT auto-complete. The approval endpoint marks
                    // COMPLETED when the reviewer approves the DevOps output (or approve-all).
                    job.setStatus(PipelineStatus.AWAITING_APPROVAL);
                    job.setCurrentStage("DEVOPS_AWAITING_APPROVAL");
                    pipelineJobRepository.save(job);
                    log.info("DevOps completed -> awaiting approval for jobId={}", message.jobId());
                }
            }
        } catch (Exception e) {
            log.error("DevOps failed for jobId={} - marking FAILED, not requeue", message.jobId(), e);
            try {
                var job = pipelineJobRepository.findById(UUID.fromString(message.jobId())).orElse(null);
                if (job != null) {
                    job.setStatus(PipelineStatus.FAILED);
                    job.setCurrentStage("FAILED_AT_DEVOPS: " + e.getMessage());
                    job.setCompletedAt(LocalDateTime.now());
                    pipelineJobRepository.save(job);
                }
            } catch (Exception ignored) {}
            // Do NOT throw - throwing would make RabbitMQ requeue/loop forever.
        }
    }

    // Step 1: Receive Task
    public String deploy(String projectInfo, String jobId) {
        log.info("DevOps Agent started for jobId: {}", jobId);
        try {
            // Step 2: Gather context from the generated code + design.
            String pom = storageService.read("jobs/" + jobId + "/code/pom.xml");
            String appYml = storageService.read("jobs/" + jobId + "/code/src/main/resources/application.yml");
            String schema = storageService.read("jobs/" + jobId + "/design/schema.sql");
            if (pom == null || pom.isBlank()) {
                throw new RuntimeException(
                        "No generated code found for jobId " + jobId
                                + " (expected jobs/" + jobId + "/code/pom.xml). "
                                + "Run the Developer Agent for this job first.");
            }
            log.info("DevOps context loaded: pom={} appYml={} schema={} chars",
                    len(pom), len(appYml), len(schema));

            // Step 3: Query Knowledge Base (DEVOPS -> security/coding standards; light here).
            String kb = knowledgeBaseService.getContextForAgent(AgentType.DEVOPS, "deployment");
            kb = trim(kb, 600);
            log.info("KB context loaded: {} chars", kb.length());

            // Step 4: Build prompt + call LLM. Feed the REAL pom/application.yml/schema so the
            // generated Dockerfile/compose match the app (port, DB name/user/pass, postgres host).
            String systemPrompt = """
                    You are a Senior DevOps Engineer at AutoSDLC Factory. Follow these standards: %s
                    Generate clean, runnable deployment configs for a Spring Boot + PostgreSQL app.
                    Output ONLY the requested files, each marked with a `### File: <path>` header line
                    followed by raw file content. No prose, no markdown fences around the content.
                    """.formatted(kb);

            // Human approval gate: fold reviewer feedback into this re-run.
            String feedback = storageService.readFeedback(jobId, "devops");
            if (feedback != null) {
                systemPrompt += "\n\nREVIEWER FEEDBACK from the previous run — incorporate these " +
                        "changes into the deployment configs:\n" + trim(feedback, 1500);
                log.info("DevOps re-run with reviewer feedback ({} chars)", feedback.length());
            }

            String userPrompt = buildUserPrompt(pom, appYml, schema);
            log.info("Calling LLM for devops via DEVOPS chain (max_tokens=3500)");
            String raw = llmClient.chat(AgentType.DEVOPS, systemPrompt, userPrompt, 3500);
            if (raw == null) raw = "";
            log.info("LLM response for devops: length={}", raw.length());

            // Step 5: Parse + save to BOTH locations + Artifact rows.
            List<GeneratedFile> files = parseFiles(raw);
            if (files.isEmpty()) {
                throw new RuntimeException("DevOps LLM returned no `### File:` blocks - nothing generated.");
            }
            List<String> saved = new ArrayList<>();
            for (GeneratedFile f : files) {
                String normalized = normalizeDevOpsPath(f);
                // Runnable in-place copy under code/ ...
                storageService.save("jobs/" + jobId + "/code/" + normalized, f.content());
                // ... and a traceable canonical copy under devops/.
                String devopsPath = "jobs/" + jobId + "/devops/" + normalized;
                storageService.save(devopsPath, f.content());
                saveArtifactRow(jobId, f, normalized, devopsPath);
                saved.add(normalized + " (" + f.content().length() + " chars)");
            }
            log.info("Saved {} devops files for jobId={} (both code/ and devops/): {}",
                    files.size(), jobId, String.join(", ", saved));

            String summary = "DevOps completed. Saved " + files.size()
                    + " deployment files (Dockerfile, docker-compose.yml, GitHub Actions CI/CD) "
                    + "to jobs/" + jobId + "/code/ and jobs/" + jobId + "/devops/.";
            log.info(summary);
            return summary;

        } catch (Exception e) {
            log.error("DevOps Agent failed for jobId: {}", jobId, e);
            throw new RuntimeException("DevOps Agent failed: " + e.getMessage(), e);
        }
    }

    // ---- prompt -------------------------------------------------------------

    /**
     * Build the user prompt. Embeds the real pom.xml (deps), application.yml (datasource + port),
     * and schema.sql (tables) so the generated configs line up with the actual app.
     */
    private String buildUserPrompt(String pom, String appYml, String schema) {
        StringBuilder sb = new StringBuilder();
        sb.append("Generate deployment configs for this Spring Boot + PostgreSQL app. " +
                "Produce EXACTLY these three files:\n\n");

        sb.append("1) Dockerfile - multi-stage build:\n");
        sb.append("   - `FROM maven:3.9-eclipse-temurin-21 AS build` then copy pom.xml + src and " +
                "run `mvn -B package -DskipTests`.\n");
        sb.append("   - `FROM eclipse-temurin:21-jre` runtime; copy the built jar from the build " +
                "stage; EXPOSE the app port; ENTRYPOINT `java -jar /app/app.jar`.\n\n");

        sb.append("2) docker-compose.yml - two services:\n");
        sb.append("   - `db` (or `postgres`): image postgres:18-alpine, with POSTGRES_DB/USER/PASSWORD " +
                "matching the app's datasource, port 5432, a named volume for data.\n");
        sb.append("   - `app`: build from the Dockerfile, ports `<appPort>:<appPort>`, env vars " +
                "SPRING_DATASOURCE_URL=jdbc:postgresql://<db-service-name>:5432/<db> + " +
                "SPRING_DATASOURCE_USERNAME/PASSWORD overriding application.yml so the app connects " +
                "to the postgres SERVICE (not localhost), depends_on the db service.\n\n");

        sb.append("3) .github/workflows/ci-cd.yml - GitHub Actions:\n");
        sb.append("   - `on: push` (and `pull_request`) to main.\n");
        sb.append("   - A `build` job: checkout, setup JDK 21, cache Maven, `mvn -B package`.\n");
        sb.append("   - A `test` job: `mvn test` (depends on build).\n");
        sb.append("   - A `deploy` job: build the Docker image and (placeholder) push to a registry " +
                "using secrets - gated on the main branch. Include a comment that deploy secrets " +
                "must be configured.\n\n");

        if (appYml != null && !appYml.isBlank())
            sb.append("App's application.yml (use its port, datasource DB name/user/password):\n")
                    .append(truncate(appYml, 1200)).append("\n\n");
        if (pom != null && !pom.isBlank())
            sb.append("App's pom.xml (deps - Spring Boot + PostgreSQL + Java 21):\n")
                    .append(truncate(pom, 1500)).append("\n\n");
        if (schema != null && !schema.isBlank())
            sb.append("Database schema (tables the postgres service must provision via ddl-auto):\n")
                    .append(truncate(schema, 1000)).append("\n\n");

        sb.append("Output rules:\n");
        sb.append("- Output ONLY the three files, each preceded by a line `### File: <path>` then raw content.\n");
        sb.append("- Paths: `Dockerfile`, `docker-compose.yml`, `.github/workflows/ci-cd.yml`.\n");
        sb.append("- Use the REAL port / DB name / user / password from application.yml above.\n");
        sb.append("- No prose, no markdown fences around the file content.\n");
        return sb.toString();
    }

    // ---- parsing + normalization -------------------------------------------

    /**
     * Parse a multi-file LLM response into individual files (same format the other agents use).
     * A single outer markdown fence around a file's content is stripped.
     */
    private List<GeneratedFile> parseFiles(String raw) {
        List<GeneratedFile> files = new ArrayList<>();
        if (raw == null || raw.isBlank()) return files;

        Matcher m = FILE_HEADER.matcher(raw);
        List<int[]> spans = new ArrayList<>();
        while (m.find()) spans.add(new int[]{m.start(), m.end()});

        if (spans.isEmpty()) {
            log.warn("No '### File:' markers found in devops LLM response - saving raw output as _raw.txt");
            String trimmed = raw.trim();
            if (!trimmed.isBlank()) files.add(new GeneratedFile("_raw.txt", trimmed));
            return files;
        }

        for (int i = 0; i < spans.size(); i++) {
            int headerStart = spans.get(i)[0];
            int headerEnd = spans.get(i)[1];
            String path = raw.substring(headerStart, headerEnd)
                    .replaceAll("(?i)^###\\s*File:\\s*", "").trim();
            int contentStart = headerEnd;
            int contentEnd = (i + 1 < spans.size()) ? spans.get(i + 1)[0] : raw.length();
            String content = stripOuterFence(raw.substring(contentStart, contentEnd).trim());
            if (!path.isBlank() && !content.isBlank()) {
                files.add(new GeneratedFile(path, content));
            }
        }
        return files;
    }

    /**
     * Normalize an LLM-emitted devops file path to a project-root-relative location.
     *  - Dockerfile -> Dockerfile (root)
     *  - docker-compose.yml/yaml -> docker-compose.yml (root)
     *  - .github/** or github/** -> .github/** (kept relative to root)
     *  - anything else -> kept at the root by basename
     */
    String normalizeDevOpsPath(GeneratedFile f) {
        String p = f.path().trim().replace('\\', '/');
        while (p.startsWith("/")) p = p.substring(1);
        String lower = p.toLowerCase();

        if (lower.endsWith("dockerfile")) return "Dockerfile";
        if (lower.equals("docker-compose.yml") || lower.equals("docker-compose.yaml")
                || lower.endsWith("/docker-compose.yml") || lower.endsWith("/docker-compose.yaml")) {
            return "docker-compose.yml";
        }
        if (lower.startsWith(".github/")) return p;
        if (lower.startsWith("github/")) return ".github/" + p.substring("github/".length());
        return basename(p);
    }

    /** Remove a single outer markdown code fence if the LLM wrapped a file in one. */
    private String stripOuterFence(String text) {
        String t = text.trim();
        if (!t.startsWith("```")) return t;
        int firstNl = t.indexOf('\n');
        if (firstNl == -1) return t;
        t = t.substring(firstNl + 1);
        int lastFence = t.lastIndexOf("```");
        if (lastFence > -1) t = t.substring(0, lastFence);
        return t.trim();
    }

    // ---- persistence --------------------------------------------------------

    private void saveArtifactRow(String jobId, GeneratedFile file, String normalizedPath, String storedPath) {
        PipelineJob job = pipelineJobRepository.findById(UUID.fromString(jobId)).orElse(null);
        Artifact artifact = Artifact.builder()
                .pipelineJob(job)
                .artifactType(ArtifactType.DEPLOYMENT)
                .fileName(normalizedPath)
                .minioPath(storedPath)
                .contentType(contentTypeFor(normalizedPath))
                .fileSize((long) file.content().length())
                .createdAt(LocalDateTime.now())
                .build();
        artifactRepository.save(artifact);
    }

    private String contentTypeFor(String path) {
        String p = path.toLowerCase();
        if (p.endsWith(".yml") || p.endsWith(".yaml")) return "application/yaml";
        if (p.endsWith(".xml")) return "application/xml";
        if (p.endsWith(".tf")) return "text/plain";
        // Dockerfile + workflows are plain text
        return "text/plain";
    }

    // ---- small helpers ------------------------------------------------------

    private String basename(String path) {
        int i = path.lastIndexOf('/');
        return i == -1 ? path : path.substring(i + 1);
    }

    private String trim(String s, int max) {
        if (s == null) return null;
        return s.length() > max ? s.substring(0, max) : s;
    }

    private String truncate(String s, int max) {
        if (s == null) return "";
        return s.length() > max ? s.substring(0, max) + "\n...[truncated]" : s;
    }

    private int len(String s) {
        return s == null ? 0 : s.length();
    }
}
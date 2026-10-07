package com.autosdlc.autosdlc_platform.service.agents;

import com.autosdlc.autosdlc_platform.config.RabbitMQConfig;
import com.autosdlc.autosdlc_platform.model.dto.DeveloperDoneMessage;
import com.autosdlc.autosdlc_platform.model.dto.DeveloperTaskMessage;
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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Developer Agent - Phase 10 (enhanced: Maven-layout normalization + self-correcting compile loop).
 *
 * Takes the Architect Agent's design (schema.sql, api-spec.yaml, lld.md) and generates
 * a complete, BUILDABLE Spring Boot application + an optimized frontend, layer by layer:
 *   Call 1: JPA entity classes (+ enums)                      -> entity/*.java
 *   Call 2: JpaRepository interfaces + @Service classes        -> repository/*.java, service/*.java
 *   Call 3: @RestController classes + DTOs + exception handler -> controller/*.java, dto/*.java, exception/*.java
 *   Call 4: Main class + application.yml + pom.xml             -> Application.java, application.yml, pom.xml
 *   Call 5: Optimized frontend (Tailwind + vanilla JS)         -> frontend/index.html, frontend/app.js
 *
 * Then it normalizes every file into a REAL Maven project layout and runs a self-correcting
 * `mvn compile` loop (compile -> feed errors back to the LLM -> overwrite broken files -> retry,
 * up to MAX_COMPILE_ATTEMPTS times). So the output under jobs/{jobId}/code/ is a directly
 * buildable Maven project, not a pile of loose files.
 *
 * Path normalization: the LLM emits `### File: <path>` with inconsistent paths (sometimes
 * `entity/Foo.java`, sometimes `com/generated/app/...`, sometimes `src/main/java/...`). We do
 * NOT trust that path. For .java files we derive the correct location from the file's own
 * `package` declaration: src/main/java/<package-as-path>/<ClassName>.java. That guarantees
 * the on-disk tree matches the packages and the project actually compiles. Config files go to
 * src/main/resources/, pom.xml to the root, frontend/ stays at the root.
 *
 * Why multiple generation calls instead of one giant prompt: a single call risks truncation
 * and gives the model less focus per layer. Splitting lets each layer get full attention AND
 * lets us feed the previous layers' actual code as context, so services match the generated
 * entities, controllers match the services, etc.
 *
 * Token optimization ("optimize every agent till max limit"): each code call gets a generous
 * max_tokens cap; KB + design context is trimmed hard to leave room for output; calls go
 * through the DEVELOPER provider chain which auto-falls back across Groq keys / local Ollama
 * on 429 (quota) so free-tier exhaustion does not block generation.
 *
 * JDK 24 compatibility: generated pom targets Spring Boot 3.5.x (runs on JDK 24) and does NOT
 * pin a Lombok version (the parent manages a JDK-24-compatible Lombok). Pinning an old Lombok
 * makes annotation processing fail on JDK 24.
 *
 * JGit commit is NOT done here (no JGit dependency yet, per "keep it simple"). A later phase
 * can `git init` + commit jobs/{jobId}/code/.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class DeveloperAgent {

    private final KnowledgeBaseService knowledgeBaseService;
    private final ArtifactStorageService storageService;
    private final LLMClient llmClient;
    private final ArtifactRepository artifactRepository;
    private final PipelineJobRepository pipelineJobRepository;
    private final RabbitTemplate rabbitTemplate;

    /** A single file parsed out of the LLM's multi-file response: its emitted path + raw content. */
    private record GeneratedFile(String path, String content) {}

    /** Header line the LLM uses to delimit each file, e.g. "### File: entity/User.java". Case-insensitive. */
    private static final Pattern FILE_HEADER =
            Pattern.compile("(?im)^###\\s*File:\\s*(.+?)\\s*$");

    /** Extract `package com.generated.app.entity;` -> `com.generated.app.entity` from a .java file. */
    private static final Pattern PACKAGE_DECL =
            Pattern.compile("(?m)^\\s*package\\s+([\\w.]+)\\s*;");

    /** Max times to run the compile -> LLM-fix -> recompile loop. */
    private static final int MAX_COMPILE_ATTEMPTS = 3;

    /** Per-process timeout for `mvn compile` (first run downloads Spring Boot deps). */
    private static final long COMPILE_TIMEOUT_MINUTES = 8L;

    /**
     * Auto-triggered when the Architect completes: the orchestrator publishes a
     * DeveloperTaskMessage to the developer queue, and this listener runs the dev phase.
     *
     * On success the job is NOT marked complete here - the developer publishes a
     * DeveloperDoneMessage so the orchestrator can advance to the Tester Agent (Phase 11).
     * The Tester marks the pipeline COMPLETED when it finishes. On failure the job is marked
     * FAILED and NOT requeued (see RabbitMQConfig.setDefaultRequeueRejected(false)).
     */
    @RabbitListener(queues = RabbitMQConfig.DEV_QUEUE)
    public void onDeveloperTask(DeveloperTaskMessage message) {
        log.info("Developer received jobId={}", message.jobId());
        try {
            develop(null, message.jobId());
            // Developer done -> hand off to the Tester Agent. Don't mark the pipeline complete;
            // the orchestrator advances to TESTER_PENDING and the Tester marks COMPLETED.
            rabbitTemplate.convertAndSend(RabbitMQConfig.EXCHANGE, RabbitMQConfig.DEV_DONE_KEY,
                    new DeveloperDoneMessage(message.jobId()));
            log.info("Developer completed -> triggering Tester for jobId={}", message.jobId());
        } catch (Exception e) {
            log.error("Developer failed for jobId={} - marking FAILED, not requeue", message.jobId(), e);
            try {
                var job = pipelineJobRepository.findById(UUID.fromString(message.jobId())).orElse(null);
                if (job != null) {
                    job.setStatus(PipelineStatus.FAILED);
                    job.setCurrentStage("FAILED_AT_DEVELOPER: " + e.getMessage());
                    job.setCompletedAt(LocalDateTime.now());
                    pipelineJobRepository.save(job);
                }
            } catch (Exception ignored) {}
            // Do NOT throw - throwing would make RabbitMQ requeue/loop forever.
        }
    }

    // Step 1: Receive Task
    public String develop(String designJson, String jobId) {
        log.info("Developer Agent started for jobId: {}", jobId);
        try {
            // Step 2: Read Design Artifacts from storage (the paths the Architect wrote to)
            String schema = storageService.read("jobs/" + jobId + "/design/schema.sql");
            String apiSpec = storageService.read("jobs/" + jobId + "/design/api-spec.yaml");
            String lld = storageService.read("jobs/" + jobId + "/design/lld.md");

            if ((lld == null || lld.isBlank()) && (schema == null || schema.isBlank())) {
                throw new RuntimeException(
                        "No design found for jobId " + jobId
                                + ". Run the Architect Agent for this job first.");
            }
            // Trim each design input hard - it is repeated across calls, and we want room for OUTPUT.
            schema = trim(schema, 1500);
            apiSpec = trim(apiSpec, 1800);
            lld = trim(lld, 1500);
            log.info("Design loaded: schema={} apiSpec={} lld={}", len(schema), len(apiSpec), len(lld));

            // Step 3: Query Knowledge Base (once - reused for every layer call)
            String kb = knowledgeBaseService.getContextForAgent(AgentType.DEVELOPER, "spring boot");
            kb = trim(kb, 800);
            log.info("KB context loaded: {} chars", kb.length());

            // Human approval gate: if the reviewer rejected a previous Developer run with
            // feedback, fold it into the generation system prompt (kbGen) so every layer call
            // incorporates it. The fix/compile loop still uses the CLEAN kb (feedback is about
            // what to generate, not about compile errors).
            String feedback = storageService.readFeedback(jobId, "developer");
            String kbGen = kb;
            if (feedback != null) {
                kbGen = kb + "\n\nREVIEWER FEEDBACK from the previous run — incorporate these " +
                        "changes into the generated code:\n" + trim(feedback, 1500);
                log.info("Developer re-run with reviewer feedback ({} chars)", feedback.length());
            }

            // codeFiles: normalized-path -> file. Preserves insertion order and lets the compile
            // loop overwrite individual files when the LLM issues a fix.
            Map<String, GeneratedFile> codeFiles = new LinkedHashMap<>();

            // Step 4-6: five focused LLM calls, each feeding the previous layers as context.

            // Call 1: Entities (+ enums). Foundation for everything else.
            String entityPrompt = layerUserPrompt(
                    "Generate ONLY the JPA entity classes (and any enum types as separate files). " +
                    "Use @Entity @Table @Id @GeneratedValue(strategy=IDENTITY) @Column, Lombok " +
                    "@Data @NoArgsConstructor @AllArgsConstructor, correct Java types for the columns, " +
                    "and jakarta.validation annotations where sensible. Package: com.generated.app.entity. " +
                    "One entity per table in the schema.",
                    schema, lld, null, null, apiSpec);
            List<GeneratedFile> entityFiles = parseFiles(callLayer("entities", kbGen, entityPrompt, 3500));
            merge(codeFiles, entityFiles);
            String entityCode = joinFiles(entityFiles);

            // Call 2: Repositories + Services. Sees the generated entities so it matches them.
            String repoSvcPrompt = layerUserPrompt(
                    "Generate ONLY: (a) JpaRepository interfaces (one per entity, extend " +
                    "JpaRepository<EntityType, Long>) in package com.generated.app.repository, and " +
                    "(b) @Service @Transactional service classes in package com.generated.app.service " +
                    "with full CRUD plus any business methods implied by the design. Each service MUST " +
                    "expose create(...), getById(...), and list() (findAll) methods so controllers can " +
                    "offer list endpoints. Use constructor injection of the repositories. " +
                    "Do NOT regenerate entities.",
                    schema, lld, entityCode, null, apiSpec);
            List<GeneratedFile> repoSvcFiles = parseFiles(callLayer("repositories+services", kbGen, repoSvcPrompt, 3500));
            merge(codeFiles, repoSvcFiles);
            String serviceCode = joinFiles(repoSvcFiles);

            // Call 3: Controllers + DTOs + exception handler. Sees entities + services.
            String ctrlPrompt = layerUserPrompt(
                    "Generate ONLY: (a) @RestController classes in package com.generated.app.controller " +
                    "mapping EVERY endpoint from the API spec, using constructor-injected services, " +
                    "@Valid on request bodies, ResponseEntity with correct status codes. For each " +
                    "collection resource expose: GET /{resource} (list all, returns a JSON array), " +
                    "GET /{resource}/{id}, and POST /{resource}. " +
                    "(b) request/response DTOs in package com.generated.app.dto (not raw entities). " +
                    "In create-request DTOs, make foreign-key / association fields (e.g. assigneeId, " +
                    "parentId, teamId) OPTIONAL (nullable, no @NotNull) unless strictly required, so " +
                    "the UI can create entities incrementally. " +
                    "(c) one global @ControllerAdvice exception handler in package " +
                    "com.generated.app.exception. Do NOT regenerate entities or services.",
                    schema, lld, entityCode, serviceCode, apiSpec);
            List<GeneratedFile> ctrlFiles = parseFiles(callLayer("controllers+dtos", kbGen, ctrlPrompt, 3500));
            merge(codeFiles, ctrlFiles);

            // Call 4: Config + main + pom. Wiring to run the app.
            String cfgPrompt = layerUserPrompt(
                    "Generate ONLY the project skeleton: (a) the Spring Boot main class " +
                    "(@SpringBootApplication, package com.generated.app, ### File: Application.java); " +
                    "(b) application.yml with PostgreSQL datasource " +
                    "(jdbc:postgresql://localhost:5432/generated_app, user/pass root/root), " +
                    "spring.jpa.hibernate.ddl-auto=update, server.port=8081. " +
                    "Do NOT generate pom.xml or any CORS/config class - those are provided " +
                    "automatically by the build system. No entities/services/controllers.",
                    schema, lld, null, null, apiSpec);
            merge(codeFiles, parseFiles(callLayer("config+main+pom", kbGen, cfgPrompt, 1500)));

            // Call 5: Optimized frontend. Talks to the REST API. It is generated against the
            // ACTUAL controllers + DTOs (not just the architect's api-spec, which can drift from
            // what the controller really exposes), so the UI only calls endpoints that exist and
            // sends exactly the fields the create DTO expects.
            String endpointCode = filesByPath(codeFiles, "/controller/")
                    + filesByPath(codeFiles, "/dto/");
            String feTask = "Generate ONLY a clean, optimized, responsive frontend as TWO files: " +
                    "(1) frontend/index.html - a single page using the Tailwind CSS CDN, a navbar, " +
                    "and a list + create form for the main resource(s); " +
                    "(2) frontend/app.js - vanilla JavaScript (no framework) using fetch() against " +
                    "http://localhost:8081 to load and submit data, with basic loading/error states. " +
                    "Call ONLY the endpoints shown in 'Actual generated endpoints' below. For each " +
                    "resource support loading the list (GET) and creating (POST), sending exactly the " +
                    "fields the create DTO expects (omit optional association fields the user can't " +
                    "supply yet). Output raw HTML and JS only, no markdown fences.";
            StringBuilder feUser = new StringBuilder();
            if (apiSpec != null && !apiSpec.isBlank())
                feUser.append("API spec:\n").append(apiSpec).append("\n\n");
            feUser.append("Actual generated controllers + DTOs (call ONLY these endpoints, ")
                  .append("match request fields exactly):\n")
                  .append(truncate(endpointCode, 2500)).append("\n\n");
            feUser.append("Task:\n").append(feTask).append("\n\n");
            feUser.append("Output rules:\n");
            feUser.append("- Output ONLY files, each preceded by a line `### File: <path>`.\n");
            feUser.append("- Paths: frontend/index.html and frontend/app.js.\n");
            feUser.append("- No prose, no markdown fences around the file content.\n");
            merge(codeFiles, parseFiles(callLayer("frontend", kbGen, feUser.toString(), 2500)));

            // Inject fixed scaffolding (pom.xml + CORS config) - critical boilerplate we do NOT
            // trust the LLM with. The pom must be byte-exact for JDK 24 (Lombok 1.18.46 +
            // explicit annotationProcessorPaths, else @Data silently generates no accessors);
            // the CORS config lets the generated frontend call this API when opened directly
            // as a file:// in a browser. Done after the LLM merges so these override any
            // pom/config the model emitted.
            injectFixedScaffolding(codeFiles);

            // Save every parsed file to storage (normalized Maven layout) + create an Artifact row.
            List<String> saved = new ArrayList<>();
            for (GeneratedFile f : codeFiles.values()) {
                String normalized = normalizePath(f);
                String rel = "jobs/" + jobId + "/code/" + normalized;
                String stored = storageService.save(rel, f.content());
                saveArtifactRow(jobId, f, normalized, stored);
                saved.add(normalized + " (" + f.content().length() + " chars)");
            }
            log.info("Saved {} files for jobId={}: {}", codeFiles.size(), jobId, String.join(", ", saved));

            // Self-correcting compile loop: turn the loose files into a real, buildable project.
            File codeDir = new File("artifacts", "jobs/" + jobId + "/code").getAbsoluteFile();
            boolean compiled = compileWithSelfCorrection(codeDir, codeFiles, kb);

            String summary = "Developer completed. Saved " + codeFiles.size()
                    + " files to a Maven project layout. Compile: "
                    + (compiled ? "SUCCESS" : "FAILED after " + MAX_COMPILE_ATTEMPTS + " attempts");
            log.info(summary + " for jobId: {}", jobId);
            return summary;

        } catch (Exception e) {
            log.error("Developer Agent failed for jobId: {}", jobId, e);
            throw new RuntimeException("Developer Agent failed: " + e.getMessage(), e);
        }
    }

    // ---- generation prompts -------------------------------------------------

    /**
     * Build the user prompt for one layer. Layers that depend on prior layers get their
     * generated code threaded in as context so the new layer actually fits the entities/services.
     */
    private String layerUserPrompt(String task, String schema, String lld,
                                   String entityCode, String serviceCode, String apiSpec) {
        StringBuilder sb = new StringBuilder();
        if (schema != null && !schema.isBlank()) sb.append("Database schema:\n").append(schema).append("\n\n");
        if (apiSpec != null && !apiSpec.isBlank()) sb.append("API spec:\n").append(apiSpec).append("\n\n");
        if (lld != null && !lld.isBlank()) sb.append("Design (LLD):\n").append(lld).append("\n\n");
        if (entityCode != null && !entityCode.isBlank())
            sb.append("Already-generated entities (do NOT regenerate, match these):\n")
                    .append(entityCode).append("\n\n");
        if (serviceCode != null && !serviceCode.isBlank())
            sb.append("Already-generated services (do NOT regenerate, inject these):\n")
                    .append(serviceCode).append("\n\n");
        sb.append("Task:\n").append(task).append("\n\n");
        sb.append("Output rules:\n");
        sb.append("- Output ONLY files, each preceded by a line `### File: <path>` then the raw content.\n");
        sb.append("- Use simple paths relative to the code root, e.g. entity/User.java, repository/UserRepository.java.\n");
        sb.append("- The build system will place each file in the correct Maven location from its package.\n");
        sb.append("- No prose, no explanations, no markdown fences around the file content.\n");
        return sb.toString();
    }

    /** Call the LLM for one layer via the DEVELOPER provider chain. Returns the raw multi-file response. */
    private String callLayer(String layerName, String kb, String userPrompt, int maxTokens) {
        String systemPrompt = """
                You are a Senior Java Developer at AutoSDLC Factory. Follow these coding standards: %s
                Tech: Java 21, Spring Boot, Spring Data JPA, PostgreSQL, Lombok. Generate clean,
                idiomatic, production-quality code. Output ONLY the requested files, each marked with
                a `### File: <path>` header line followed by raw file content. No prose, no fences.
                """.formatted(kb);

        log.info("Calling LLM for {} via DEVELOPER chain (max_tokens={})", layerName, maxTokens);
        String raw = llmClient.chat(AgentType.DEVELOPER, systemPrompt, userPrompt, maxTokens);
        if (raw == null) raw = "";
        log.info("LLM response for {}: length={}", layerName, raw.length());
        return raw;
    }

    // ---- parsing + normalization -------------------------------------------

    /**
     * Parse a multi-file LLM response into individual files. Each file is introduced by a
     * `### File: <path>` line; its content runs until the next header (or end of response).
     * A single outer markdown fence around a file's content (if the LLM adds one) is stripped.
     */
    private List<GeneratedFile> parseFiles(String raw) {
        List<GeneratedFile> files = new ArrayList<>();
        if (raw == null || raw.isBlank()) return files;

        Matcher m = FILE_HEADER.matcher(raw);
        List<int[]> spans = new ArrayList<>();
        while (m.find()) spans.add(new int[]{m.start(), m.end()});

        if (spans.isEmpty()) {
            // No file markers - the model ignored the format. Save the whole blob as one
            // fallback file so nothing is silently lost, and log loudly.
            log.warn("No '### File:' markers found in LLM response - saving raw output as _raw.txt");
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
     * Map an LLM-emitted file path to its correct location in a Maven project.
     *  - .java files: src/main/java/&lt;package-as-path&gt;/&lt;ClassName&gt;.java (derived from
     *    the file's `package` declaration, NOT the emitted path - the LLM's paths are unreliable).
     *  - application.yml/yaml/properties: src/main/resources/&lt;name&gt;
     *  - pom.xml: root
     *  - frontend/**: kept under frontend/ at the root
     *  - anything else: kept at the root by basename
     */
    String normalizePath(GeneratedFile f) {
        String p = f.path().trim().replace('\\', '/');
        while (p.startsWith("/")) p = p.substring(1);
        String lower = p.toLowerCase();

        if (lower.endsWith(".java")) {
            String pkg = extractPackage(f.content());
            String cls = basename(p);
            if (pkg != null && !pkg.isBlank()) {
                return "src/main/java/" + pkg.replace('.', '/') + "/" + cls;
            }
            // No package line: keep under src/main/java using the emitted relative path.
            if (lower.startsWith("src/main/java/")) return p;
            return "src/main/java/" + p;
        }
        if (lower.endsWith("application.yml") || lower.endsWith("application.yaml")
                || lower.endsWith("application.properties")) {
            return "src/main/resources/" + basename(p);
        }
        if (lower.endsWith("pom.xml")) {
            return "pom.xml";
        }
        if (lower.startsWith("frontend/")) {
            return p;
        }
        return basename(p);
    }

    /** Put a batch of parsed files into the map keyed by normalized path (later ones overwrite). */
    private void merge(Map<String, GeneratedFile> codeFiles, List<GeneratedFile> batch) {
        for (GeneratedFile f : batch) {
            codeFiles.put(normalizePath(f), f);
        }
    }

    /** Extract the package name from a .java source string, or null if none. */
    private String extractPackage(String javaSource) {
        Matcher m = PACKAGE_DECL.matcher(javaSource);
        return m.find() ? m.group(1) : null;
    }

    // ---- fixed scaffolding (NOT LLM-generated) --------------------------------

    /**
     * Inject the fixed pom.xml and CorsConfig.java. These are critical boilerplate that must be
     * byte-exact for the generated project to (a) compile on JDK 24 - Lombok 1.18.46 + an explicit
     * maven-compiler-plugin annotationProcessorPaths entry, without which @Data/@Getter/@Setter
     * silently generate NO accessors and the project fails with mass "cannot find symbol" errors;
     * and (b) be callable from a frontend opened directly in a browser (file://) via permissive
     * CORS. Trusting the LLM with these has proven unreliable, so we override/insert them here.
     * Keyed by the normalized path so the save loop and compile-fix loop treat them like any file.
     */
    private void injectFixedScaffolding(Map<String, GeneratedFile> codeFiles) {
        codeFiles.put("pom.xml", new GeneratedFile("pom.xml", pomXml()));
        codeFiles.put("src/main/java/com/generated/app/config/CorsConfig.java",
                new GeneratedFile("config/CorsConfig.java", corsConfigJava()));
    }

    /** Fixed pom: Spring Boot 3.5.3 (JDK-24-certified), Java 21, Lombok 1.18.46 on the processor path. */
    private String pomXml() {
        return """
                <?xml version="1.0" encoding="UTF-8"?>
                <project xmlns="http://maven.apache.org/POM/4.0.0"
                         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
                    <modelVersion>4.0.0</modelVersion>

                    <parent>
                        <groupId>org.springframework.boot</groupId>
                        <artifactId>spring-boot-starter-parent</artifactId>
                        <version>3.5.3</version>
                        <relativePath/>
                    </parent>

                    <groupId>com.generated.app</groupId>
                    <artifactId>generated-app</artifactId>
                    <version>0.0.1-SNAPSHOT</version>
                    <name>generated-app</name>

                    <properties>
                        <java.version>21</java.version>
                        <!-- JDK-24-compatible Lombok; the parent's managed version is too old. -->
                        <lombok.version>1.18.46</lombok.version>
                    </properties>

                    <dependencies>
                        <dependency>
                            <groupId>org.springframework.boot</groupId>
                            <artifactId>spring-boot-starter-data-jpa</artifactId>
                        </dependency>
                        <dependency>
                            <groupId>org.springframework.boot</groupId>
                            <artifactId>spring-boot-starter-validation</artifactId>
                        </dependency>
                        <dependency>
                            <groupId>org.springframework.boot</groupId>
                            <artifactId>spring-boot-starter-web</artifactId>
                        </dependency>
                        <dependency>
                            <groupId>org.postgresql</groupId>
                            <artifactId>postgresql</artifactId>
                            <scope>runtime</scope>
                        </dependency>
                        <dependency>
                            <groupId>org.projectlombok</groupId>
                            <artifactId>lombok</artifactId>
                            <version>${lombok.version}</version>
                            <optional>true</optional>
                        </dependency>
                        <!-- Test stack: JUnit 5, Mockito, MockMvc, AssertJ (via starter-test) +
                             H2 in-memory so `mvn test` runs without an external Postgres. -->
                        <dependency>
                            <groupId>org.springframework.boot</groupId>
                            <artifactId>spring-boot-starter-test</artifactId>
                            <scope>test</scope>
                        </dependency>
                        <dependency>
                            <groupId>com.h2database</groupId>
                            <artifactId>h2</artifactId>
                            <scope>test</scope>
                        </dependency>
                    </dependencies>

                    <build>
                        <plugins>
                            <plugin>
                                <groupId>org.springframework.boot</groupId>
                                <artifactId>spring-boot-maven-plugin</artifactId>
                                <configuration>
                                    <excludes>
                                        <exclude>
                                            <groupId>org.projectlombok</groupId>
                                            <artifactId>lombok</artifactId>
                                        </exclude>
                                    </excludes>
                                </configuration>
                            </plugin>
                            <plugin>
                                <groupId>org.apache.maven.plugins</groupId>
                                <artifactId>maven-compiler-plugin</artifactId>
                                <configuration>
                                    <annotationProcessorPaths>
                                        <path>
                                            <groupId>org.projectlombok</groupId>
                                            <artifactId>lombok</artifactId>
                                            <version>${lombok.version}</version>
                                        </path>
                                    </annotationProcessorPaths>
                                </configuration>
                            </plugin>
                        </plugins>
                    </build>
                </project>
                """.trim();
    }

    /**
     * Fixed permissive CORS config. allowedOriginPatterns("*") + allowCredentials(true) is the
     * combo Spring requires to support credentialed cross-origin requests, and it matches the
     * `null` origin a browser sends when a page is opened directly via file:// - so the generated
     * frontend works by just double-clicking index.html. Demo-only; tighten for production.
     */
    private String corsConfigJava() {
        return """
                package com.generated.app.config;

                import org.springframework.context.annotation.Configuration;
                import org.springframework.web.servlet.config.annotation.CorsRegistry;
                import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

                /**
                 * Permissive CORS so the generated frontend (opened directly as a file in a
                 * browser, which sends Origin: null) can call this API at http://localhost:8081.
                 * Demo-only: allow all origins, methods, and headers. Tighten for production.
                 */
                @Configuration
                public class CorsConfig implements WebMvcConfigurer {
                    @Override
                    public void addCorsMappings(CorsRegistry registry) {
                        registry.addMapping("/**")
                                .allowedOriginPatterns("*")
                                .allowedMethods("*")
                                .allowedHeaders("*")
                                .allowCredentials(true);
                    }
                }
                """.trim();
    }

    /**
     * Remove a single outer markdown code fence (e.g. ```java ... ```) if the LLM wrapped a
     * file in one despite instructions. Leaves inner fences untouched.
     */
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

    /** Concatenate parsed files back into a single context string for the next layer's prompt. */
    private String joinFiles(List<GeneratedFile> files) {
        StringBuilder sb = new StringBuilder();
        for (GeneratedFile f : files) {
            sb.append("### File: ").append(f.path()).append("\n")
                    .append(f.content()).append("\n\n");
        }
        return sb.toString();
    }

    /** Concatenate the content of all files whose normalized path contains the given substring. */
    private String filesByPath(Map<String, GeneratedFile> codeFiles, String contains) {
        StringBuilder sb = new StringBuilder();
        for (var entry : codeFiles.entrySet()) {
            if (entry.getKey().contains(contains)) {
                sb.append("### File: ").append(entry.getValue().path()).append("\n")
                        .append(entry.getValue().content()).append("\n\n");
            }
        }
        return sb.toString();
    }

    // ---- compile loop -------------------------------------------------------

    /**
     * Self-correcting compile: copy the Maven wrapper into the generated project, run `mvn compile`,
     * and on failure feed the errors + current file contents back to the LLM, overwrite the files
     * it returns, and retry. Up to MAX_COMPILE_ATTEMPTS times. Returns true if it eventually compiled.
     */
    private boolean compileWithSelfCorrection(File codeDir, Map<String, GeneratedFile> codeFiles, String kb) {
        copyMavenWrapper(codeDir);

        for (int attempt = 1; attempt <= MAX_COMPILE_ATTEMPTS; attempt++) {
            CompileResult r = runMvnCompile(codeDir);
            log.info("Compile attempt {}/{}: exit={} ({} chars output)", attempt, MAX_COMPILE_ATTEMPTS, r.exit, r.output.length());

            if (r.exit == 0) {
                log.info("Generated code COMPILED on attempt {}", attempt);
                return true;
            }
            if (attempt == MAX_COMPILE_ATTEMPTS) {
                log.warn("Code did not compile after {} attempts. Last errors:\n{}",
                        MAX_COMPILE_ATTEMPTS, truncate(r.output, 2000));
                return false;
            }

            log.warn("Compile attempt {} failed - asking LLM to fix. Errors:\n{}",
                    attempt, truncate(r.output, 1500));

            String fixedRaw = llmClient.chat(AgentType.DEVELOPER,
                    fixSystemPrompt(kb),
                    fixUserPrompt(codeFiles.values(), r.output),
                    3500);
            List<GeneratedFile> fixedFiles = parseFiles(fixedRaw);
            if (fixedFiles.isEmpty()) {
                log.warn("LLM returned no fixable files on attempt {} - retrying compile as-is", attempt);
                continue;
            }
            // Overwrite the fixed files on disk + in the map (keyed by normalized path).
            for (GeneratedFile f : fixedFiles) {
                String normalized = normalizePath(f);
                codeFiles.put(normalized, f);
                storageService.save("jobs/" + jobId(codeDir) + "/code/" + normalized, f.content());
                log.info("LLM fix overwrote {}", normalized);
            }
        }
        return false;
    }

    /** Run `mvnw -q compile` in the generated project directory. */
    private CompileResult runMvnCompile(File codeDir) {
        List<String> cmd = System.getProperty("os.name").toLowerCase().contains("win")
                ? List.of("cmd", "/c", "mvnw.cmd", "-q", "compile")
                : List.of("./mvnw", "-q", "compile");
        ProcessBuilder pb = new ProcessBuilder(cmd).directory(codeDir).redirectErrorStream(true);
        long t0 = System.currentTimeMillis();
        try {
            Process p = pb.start();
            String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            boolean finished = p.waitFor(COMPILE_TIMEOUT_MINUTES, TimeUnit.MINUTES);
            long millis = System.currentTimeMillis() - t0;
            if (!finished) {
                p.destroyForcibly();
                return new CompileResult(-1, "TIMEOUT after " + COMPILE_TIMEOUT_MINUTES + " min\n" + truncate(out, 1500), millis);
            }
            return new CompileResult(p.exitValue(), out, millis);
        } catch (Exception e) {
            return new CompileResult(-1, "Failed to run mvn: " + e.getMessage(), System.currentTimeMillis() - t0);
        }
    }

    /**
     * Copy the platform's Maven wrapper (mvnw, mvnw.cmd, .mvn/wrapper/maven-wrapper.properties)
     * into the generated project so it can build standalone. This is exactly what Spring
     * Initializr ships. The wrapper reuses the Maven already downloaded for the platform
     * (same distributionUrl), so no Maven re-download - only the project's own deps download.
     */
    private void copyMavenWrapper(File codeDir) {
        File base = new File(".").getAbsoluteFile();
        try {
            copyFile(new File(base, "mvnw"), new File(codeDir, "mvnw"));
            copyFile(new File(base, "mvnw.cmd"), new File(codeDir, "mvnw.cmd"));
            File wrapperDir = new File(codeDir, ".mvn/wrapper");
            if (wrapperDir.mkdirs() || wrapperDir.exists()) {
                copyFile(new File(base, ".mvn/wrapper/maven-wrapper.properties"),
                        new File(wrapperDir, "maven-wrapper.properties"));
            }
            log.info("Copied Maven wrapper into {}", codeDir);
        } catch (IOException e) {
            log.warn("Could not copy Maven wrapper into generated project (compile loop will likely fail): {}", e.getMessage());
        }
    }

    private static void copyFile(File src, File dst) throws IOException {
        if (!src.exists()) return;
        Files.copy(src.toPath(), dst.toPath(), StandardCopyOption.REPLACE_EXISTING);
    }

    private String fixSystemPrompt(String kb) {
        return """
                You are a Senior Java Developer fixing compilation errors in a generated Spring Boot project.
                Standards: %s
                You will receive the current source files and the `mvn compile` error output. Output
                corrected versions of ONLY the files that need changes, each marked with a
                `### File: <path>` header followed by raw content. Use the SAME path the file was
                emitted with (e.g. entity/Task.java). Do NOT output unchanged files. No prose, no fences.
                Fix the root cause, not the symptom. Ensure imports, types, and method names line up
                across files.
                """.formatted(kb);
    }

    private String fixUserPrompt(java.util.Collection<GeneratedFile> files, String errors) {
        StringBuilder sb = new StringBuilder();
        sb.append("Compile errors:\n").append(truncate(errors, 2000)).append("\n\n");
        sb.append("Current files:\n");
        for (GeneratedFile f : files) {
            sb.append("### File: ").append(f.path()).append("\n")
                    .append(truncate(f.content(), 1500)).append("\n\n");
        }
        sb.append("Output corrected versions of the files that need fixing, each marked `### File: <path>`.\n");
        sb.append("Use the SAME path as the original. No prose, no fences.\n");
        return sb.toString();
    }

    // ---- persistence --------------------------------------------------------

    private void saveArtifactRow(String jobId, GeneratedFile file, String normalizedPath, String storedPath) {
        PipelineJob job = pipelineJobRepository.findById(UUID.fromString(jobId)).orElse(null);
        Artifact artifact = Artifact.builder()
                .pipelineJob(job)
                .artifactType(ArtifactType.SOURCE_CODE)
                .fileName(normalizedPath)
                .minioPath(storedPath)
                .contentType(contentTypeFor(normalizedPath))
                .fileSize((long) file.content().length())
                .createdAt(LocalDateTime.now())
                .build();
        artifactRepository.save(artifact);
    }

    /** Map a file extension to a content type for the artifacts table. */
    private String contentTypeFor(String path) {
        String p = path.toLowerCase();
        if (p.endsWith(".java")) return "text/x-java-source";
        if (p.endsWith(".yml") || p.endsWith(".yaml")) return "application/yaml";
        if (p.endsWith(".html")) return "text/html";
        if (p.endsWith(".js")) return "text/javascript";
        if (p.endsWith(".css")) return "text/css";
        if (p.endsWith(".xml")) return "application/xml";
        if (p.endsWith(".properties")) return "text/plain";
        return "text/plain";
    }

    // ---- small helpers ------------------------------------------------------

    /** Derive the jobId from a code dir whose path is artifacts/jobs/<jobId>/code. */
    private String jobId(File codeDir) {
        File jobsDir = codeDir.getParentFile();
        return jobsDir != null ? jobsDir.getName() : "unknown";
    }

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

    /** Result of one `mvn compile` run. */
    private record CompileResult(int exit, String output, long millis) {}
}
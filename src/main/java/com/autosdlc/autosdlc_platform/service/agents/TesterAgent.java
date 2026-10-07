package com.autosdlc.autosdlc_platform.service.agents;

import com.autosdlc.autosdlc_platform.config.RabbitMQConfig;
import com.autosdlc.autosdlc_platform.model.dto.TesterDoneMessage;
import com.autosdlc.autosdlc_platform.model.dto.TesterTaskMessage;
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
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Tester Agent - Phase 11.
 *
 * Takes the Developer Agent's generated Spring Boot project (jobs/{jobId}/code/) and generates a
 * comprehensive test suite that COMPILES against the real code, layer by layer:
 *   Call 1: Test data builders/factories            -> src/test/java/.../test/*Factory.java
 *   Call 2: Unit tests for each service method      -> src/test/java/.../<layer>/<X>ServiceTest.java
 *   Call 3: Integration tests for each controller   -> src/test/java/.../<layer>/<X>ControllerIT.java
 *
 * Each call feeds the prior layers' generated code (and the actual main source) as context, so
 * tests match the REAL signatures - not what the LLM guesses. Tests are written into the SAME
 * Maven project under src/test/java/ (path derived from each file's `package` declaration), so
 * `mvn test` actually runs them - they are not dumped into a loose tests/ folder.
 *
 * Fixed scaffolding (NOT LLM-generated), exactly the pattern the Developer Agent uses:
 *   - If the project's pom.xml lacks the test stack (spring-boot-starter-test + H2), inject it.
 *   - Write a deterministic src/test/resources/application-test.yml (H2 in-memory, create-drop)
 *     so integration tests run with @ActiveProfiles("test") WITHOUT needing an external Postgres.
 *
 * Self-correcting test-compile loop: copy the Maven wrapper, run `mvn test-compile` (compiles main
 * AND test sources; does NOT run tests), and on failure feed the errors + current test files back
 * to the LLM, overwrite the test files it returns, and retry - up to MAX_COMPILE_ATTEMPTS times.
 * We only run `test-compile`, not `mvn test`, so we don't auto-boot Spring contexts / hit the DB
 * during generation; running the suite is a manual `mvn test` the user does on demand.
 *
 * In the fix loop the LLM is told to output ONLY test files; we additionally keep only files whose
 * class name ends in Test/Tests/IT, so a stray "fixed" main source can never overwrite real code.
 *
 * Wiring: the Developer Agent publishes a DeveloperDoneMessage when it finishes; the orchestrator
 * advances to TESTER_PENDING and publishes a TesterTaskMessage to the tester queue; this listener
 * runs the test phase and marks the job COMPLETED (end of the built chain) or FAILED. Failures are
 * NOT requeued (RabbitMQConfig.setDefaultRequeueRejected(false)).
 *
 * Token optimization: generous per-call max_tokens; context is trimmed hard; calls go through the
 * TESTER provider chain (groq1,local) which auto-falls back across free-tier Groq keys / local
 * Ollama on 429 (quota) so free-tier exhaustion does not block generation.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class TesterAgent {

    private final KnowledgeBaseService knowledgeBaseService;
    private final ArtifactStorageService storageService;
    private final LLMClient llmClient;
    private final ArtifactRepository artifactRepository;
    private final PipelineJobRepository pipelineJobRepository;
    private final RabbitTemplate rabbitTemplate;

    /** A single file parsed out of the LLM's multi-file response: its emitted path + raw content. */
    private record GeneratedFile(String path, String content) {}

    /** Header line the LLM uses to delimit each file, e.g. "### File: service/FooServiceTest.java". */
    private static final Pattern FILE_HEADER =
            Pattern.compile("(?im)^###\\s*File:\\s*(.+?)\\s*$");

    /** Extract `package com.generated.app.service;` -> `com.generated.app.service` from a .java file. */
    private static final Pattern PACKAGE_DECL =
            Pattern.compile("(?m)^\\s*package\\s+([\\w.]+)\\s*;");

    /** Max times to run the test-compile -> LLM-fix -> recompile loop. */
    private static final int MAX_COMPILE_ATTEMPTS = 3;

    /** Per-process timeout for `mvn test-compile` (first run may download the test deps). */
    private static final long COMPILE_TIMEOUT_MINUTES = 8L;

    /** Test class suffixes we accept in the fix loop, so main source is never overwritten. */
    private static final Pattern TEST_CLASS_NAME =
            Pattern.compile("(?i).+(Test|Tests|IT)\\.java$");

    /**
     * Auto-triggered when the Developer completes: the orchestrator publishes a TesterTaskMessage
     * to the tester queue, and this listener runs the test phase. On success the job is NOT marked
     * complete here - the tester publishes a TesterDoneMessage so the orchestrator can advance to
     * the DevOps Agent (Phase 12), which marks the pipeline COMPLETED. On failure the job is marked
     * FAILED and NOT requeued (see RabbitMQConfig.setDefaultRequeueRejected(false)).
     */
    @RabbitListener(queues = RabbitMQConfig.TEST_QUEUE)
    public void onTesterTask(TesterTaskMessage message) {
        log.info("Tester received jobId={}", message.jobId());
        try {
            test(null, message.jobId());
            // Tester done -> hand off to the DevOps Agent. Don't mark the pipeline complete;
            // the orchestrator advances to DEVOPS_PENDING and DevOps marks COMPLETED.
            rabbitTemplate.convertAndSend(RabbitMQConfig.EXCHANGE, RabbitMQConfig.TEST_DONE_KEY,
                    new TesterDoneMessage(message.jobId()));
            log.info("Tester completed -> triggering DevOps for jobId={}", message.jobId());
        } catch (Exception e) {
            log.error("Tester failed for jobId={} - marking FAILED, not requeue", message.jobId(), e);
            try {
                var job = pipelineJobRepository.findById(UUID.fromString(message.jobId())).orElse(null);
                if (job != null) {
                    job.setStatus(PipelineStatus.FAILED);
                    job.setCurrentStage("FAILED_AT_TESTER: " + e.getMessage());
                    job.setCompletedAt(LocalDateTime.now());
                    pipelineJobRepository.save(job);
                }
            } catch (Exception ignored) {}
            // Do NOT throw - throwing would make RabbitMQ requeue/loop forever.
        }
    }

    // Step 1: Receive Task
    public String test(String codeFilesJson, String jobId) {
        log.info("Tester Agent started for jobId: {}", jobId);
        try {
            // Step 2: Read the generated code from the code project (the Developer's output).
            File codeDir = new File("artifacts", "jobs/" + jobId + "/code").getAbsoluteFile();
            if (!codeDir.exists() || !new File(codeDir, "pom.xml").exists()) {
                throw new RuntimeException(
                        "No generated code found for jobId " + jobId
                                + " (expected jobs/" + jobId + "/code/pom.xml). "
                                + "Run the Developer Agent for this job first.");
            }

            // All main .java files (relpath -> content), sorted for deterministic ordering.
            Map<String, String> mainFiles = readMainJava(codeDir);
            if (mainFiles.isEmpty()) {
                throw new RuntimeException("No .java sources found under " + codeDir + "/src/main/java");
            }
            String appYml = storageService.read("jobs/" + jobId + "/code/src/main/resources/application.yml");
            log.info("Generated code loaded: {} java files, application.yml={} chars",
                    mainFiles.size(), len(appYml));

            // Slice the main code by layer so each test call sees exactly what it must match.
            String entityCode = concatByContains(mainFiles, "/entity/");
            String serviceCode = concatByContains(mainFiles, "/service/");
            String controllerCode = concatByContains(mainFiles, "/controller/");
            String dtoCode = concatByContains(mainFiles, "/dto/");

            // Step 3: Query Knowledge Base (testing standards: Given-When-Then, JUnit5 + Mockito, ...).
            String kb = knowledgeBaseService.getContextForAgent(AgentType.TESTER, "testing");
            kb = trim(kb, 800);
            log.info("KB context loaded: {} chars", kb.length());

            // Human approval gate: fold reviewer feedback into the generation system prompt
            // (kbGen) so every test layer incorporates it. The test-compile fix loop keeps the
            // CLEAN kb (feedback is about what tests to write, not about compile errors).
            String feedback = storageService.readFeedback(jobId, "tester");
            String kbGen = kb;
            if (feedback != null) {
                kbGen = kb + "\n\nREVIEWER FEEDBACK from the previous run — incorporate these " +
                        "changes into the generated tests:\n" + trim(feedback, 1500);
                log.info("Tester re-run with reviewer feedback ({} chars)", feedback.length());
            }

            // testFiles: normalized-path -> file. The compile loop overwrites entries on LLM fixes.
            Map<String, GeneratedFile> testFiles = new LinkedHashMap<>();

            // Step 4-5: three focused LLM calls, each feeding the previous layers + the real source.

            // Call 1: Test data builders/factories. Foundation the tests reuse.
            String factoryPrompt = testUserPrompt(
                    "Generate ONLY test data builder/factory classes - one per entity and one per " +
                    "request DTO - that construct valid, fully-populated instances for tests. " +
                    "Use simple static builder methods (e.g. TaskFactory.aTask().withTitle(...).build()) " +
                    "or plain factory methods (e.g. TestDataFactory.task(...)). No JUnit, no Mockito. " +
                    "Put factories in package com.generated.app.test. Each factory must use the ACTUAL " +
                    "entity/DTO fields and setters/constructors shown below.",
                    entityCode, dtoCode, null, null);
            List<GeneratedFile> factoryFiles =
                    parseFiles(callLayer("test-factories", kbGen, factoryPrompt, 2000));
            merge(testFiles, factoryFiles);
            String factoryCode = joinFiles(factoryFiles);

            // Call 2: Unit tests for each service method. Mockito mocks repos; asserts behavior.
            String unitPrompt = testUserPrompt(
                    "Generate ONLY JUnit 5 unit tests for EVERY service class and EVERY public method. " +
                    "Annotate the class with @ExtendWith(MockitoExtension.class). Use @Mock on repository " +
                    "fields and @InjectMocks on the service, or mock(...) inline. Follow Given-When-Then " +
                    "(use comments or // given // when // then). Each service method MUST have at least " +
                    "one success case AND one failure/edge case. Use Mockito.lenient() only if needed. " +
                    "Assert with assertEquals, assertTrue, assertFalse, assertThrows, verify. " +
                    "Stub repos with when(...).thenReturn(...)/thenThrow(...)/thenReturn(Optional.empty()). " +
                    "Match the ACTUAL service method names + repo method names + entity accessors below. " +
                    "Reuse the test factories from 'Already-generated test factories' where helpful. " +
                    "Test class goes in the SAME package as the service under test " +
                    "(e.g. com.generated.app.service), named <ServiceName>Test.java. Do NOT use a " +
                    "Spring context (no @SpringBootTest) - pure Mockito unit tests.",
                    entityCode, null, serviceCode, factoryCode);
            merge(testFiles, parseFiles(callLayer("unit-tests", kbGen, unitPrompt, 3500)));

            // Call 3: Integration tests for each controller. Full Spring context + MockMvc + H2.
            String integrationPrompt = testUserPrompt(
                    "Generate ONLY JUnit 5 integration tests for EVERY @RestController, testing each " +
                    "endpoint (GET list, GET by id, POST at minimum; PUT/DELETE if they exist). Annotate " +
                    "the class with @SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK), " +
                    "@AutoConfigureMockMvc, AND @ActiveProfiles(\"test\") so the H2 in-memory test " +
                    "datasource is used (no external Postgres). Autowire MockMvc. For each endpoint write " +
                    "Given-When-Then: seed the H2 DB via the repository or the factories, perform " +
                    "mockMvc.perform(...), and assert status (is2xxSuccessful/isCreated/isOk/isBadRequest/" +
                    "isNotFound) and JSON content with jsonPath(...). Match the ACTUAL controller paths + " +
                    "request DTO fields + response DTO fields below. Test class goes in the SAME package " +
                    "as the controller (e.g. com.generated.app.controller), named <ControllerName>IT.java. " +
                    "Reuse the test factories from 'Already-generated test factories' to build request bodies.",
                    dtoCode, null, controllerCode, factoryCode);
            merge(testFiles, parseFiles(callLayer("integration-tests", kbGen, integrationPrompt, 3500)));

            // Inject fixed test scaffolding: test deps into pom (if missing) + H2 application-test.yml.
            ensureTestScaffolding(codeDir);

            // Save every parsed test file (normalized Maven test layout) + create Artifact rows.
            List<String> saved = new ArrayList<>();
            for (GeneratedFile f : testFiles.values()) {
                String normalized = normalizeTestPath(f);
                String rel = "jobs/" + jobId + "/code/" + normalized;
                String stored = storageService.save(rel, f.content());
                saveArtifactRow(jobId, f, normalized, stored);
                saved.add(normalized + " (" + f.content().length() + " chars)");
            }
            log.info("Saved {} test files for jobId={}: {}", testFiles.size(), jobId, String.join(", ", saved));

            // Self-correcting test-compile loop: compile main + test sources, fix on failure.
            boolean compiled = compileTestsWithSelfCorrection(codeDir, testFiles, kb);

            String summary = "Tester completed. Saved " + testFiles.size()
                    + " test files under src/test/java. Test-compile: "
                    + (compiled ? "SUCCESS" : "FAILED after " + MAX_COMPILE_ATTEMPTS + " attempts");
            log.info(summary + " for jobId: {}", jobId);
            return summary;

        } catch (Exception e) {
            log.error("Tester Agent failed for jobId: {}", jobId, e);
            throw new RuntimeException("Tester Agent failed: " + e.getMessage(), e);
        }
    }

    // ---- generation prompts -------------------------------------------------

    /**
     * Build the user prompt for one test layer. Threads the relevant main-source slices + any
     * already-generated test factories in as context, so tests match the real signatures.
     */
    private String testUserPrompt(String task, String entityCode, String dtoCode,
                                  String serviceOrControllerCode, String factoryCode) {
        StringBuilder sb = new StringBuilder();
        if (entityCode != null && !entityCode.isBlank())
            sb.append("Actual entities (match these fields/accessors exactly):\n")
                    .append(truncate(entityCode, 1500)).append("\n\n");
        if (dtoCode != null && !dtoCode.isBlank())
            sb.append("Actual DTOs (match these fields exactly):\n")
                    .append(truncate(dtoCode, 1500)).append("\n\n");
        if (serviceOrControllerCode != null && !serviceOrControllerCode.isBlank())
            sb.append("Actual code under test (match these method names/signatures exactly):\n")
                    .append(truncate(serviceOrControllerCode, 2500)).append("\n\n");
        if (factoryCode != null && !factoryCode.isBlank())
            sb.append("Already-generated test factories (reuse these; do NOT regenerate):\n")
                    .append(truncate(factoryCode, 1500)).append("\n\n");
        sb.append("Task:\n").append(task).append("\n\n");
        sb.append("Output rules:\n");
        sb.append("- Output ONLY test files, each preceded by a line `### File: <path>` then the raw content.\n");
        sb.append("- Use simple paths relative to the test root, e.g. service/FooServiceTest.java, " +
                "controller/FooControllerIT.java, test/TaskFactory.java.\n");
        sb.append("- The build system will place each file in src/test/java from its package declaration.\n");
        sb.append("- Every test class MUST have a `package` declaration matching its layer.\n");
        sb.append("- No prose, no explanations, no markdown fences around the file content.\n");
        return sb.toString();
    }

    /** Call the LLM for one test layer via the TESTER provider chain. Returns the raw multi-file response. */
    private String callLayer(String layerName, String kb, String userPrompt, int maxTokens) {
        String systemPrompt = """
                You are a Senior QA Automation Engineer at AutoSDLC Factory. Follow these testing standards: %s
                Tech: Java 21, Spring Boot, Spring Data JPA, JUnit 5, Mockito, MockMvc, AssertJ.
                Generate clean, COMPILABLE tests that match the ACTUAL signatures of the provided source.
                Output ONLY the requested test files, each marked with a `### File: <path>` header line
                followed by raw file content. No prose, no markdown fences.
                """.formatted(kb);

        log.info("Calling LLM for {} via TESTER chain (max_tokens={})", layerName, maxTokens);
        String raw = llmClient.chat(AgentType.TESTER, systemPrompt, userPrompt, maxTokens);
        if (raw == null) raw = "";
        log.info("LLM response for {}: length={}", layerName, raw.length());
        return raw;
    }

    // ---- reading the generated code ----------------------------------------

    /**
     * Walk jobs/{jobId}/code/src/main/java and read every .java file into a sorted map of
     * relpath -> content. relpath is relative to src/main/java (e.g. service/TaskService.java).
     */
    private Map<String, String> readMainJava(File codeDir) {
        Map<String, String> files = new TreeMap<>();
        File javaRoot = new File(codeDir, "src/main/java");
        if (!javaRoot.exists()) return files;
        walkJava(javaRoot, javaRoot, files);
        return files;
    }

    private void walkJava(File root, File dir, Map<String, String> out) {
        File[] children = dir.listFiles();
        if (children == null) return;
        for (File f : children) {
            if (f.isDirectory()) {
                walkJava(root, f, out);
            } else if (f.getName().endsWith(".java")) {
                String rel = root.toPath().relativize(f.toPath()).toString().replace('\\', '/');
                try {
                    out.put(rel, Files.readString(f.toPath()));
                } catch (IOException e) {
                    log.warn("Could not read generated source {}: {}", f, e.getMessage());
                }
            }
        }
    }

    /** Concatenate the content of all main files whose relpath contains the given substring. */
    private String concatByContains(Map<String, String> files, String contains) {
        StringBuilder sb = new StringBuilder();
        for (var entry : files.entrySet()) {
            if (entry.getKey().contains(contains)) {
                sb.append("### File: ").append(entry.getKey()).append("\n")
                        .append(entry.getValue()).append("\n\n");
            }
        }
        return sb.toString();
    }

    // ---- parsing + normalization -------------------------------------------

    /**
     * Parse a multi-file LLM response into individual files. Each file is introduced by a
     * `### File: <path>` line; its content runs until the next header (or end of response).
     * A single outer markdown fence around a file's content is stripped.
     */
    private List<GeneratedFile> parseFiles(String raw) {
        List<GeneratedFile> files = new ArrayList<>();
        if (raw == null || raw.isBlank()) return files;

        Matcher m = FILE_HEADER.matcher(raw);
        List<int[]> spans = new ArrayList<>();
        while (m.find()) spans.add(new int[]{m.start(), m.end()});

        if (spans.isEmpty()) {
            log.warn("No '### File:' markers found in tester LLM response - saving raw output as _raw.txt");
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
     * Map an LLM-emitted test file path to its correct location in the Maven TEST tree.
     *  - .java files: src/test/java/<package-as-path>/<ClassName>.java (derived from the file's
     *    `package` declaration - tests live in the SAME package as the code under test).
     *  - anything else: kept under src/test/resources/<basename>.
     */
    String normalizeTestPath(GeneratedFile f) {
        String p = f.path().trim().replace('\\', '/');
        while (p.startsWith("/")) p = p.substring(1);
        String lower = p.toLowerCase();

        if (lower.endsWith(".java")) {
            String pkg = extractPackage(f.content());
            String cls = basename(p);
            if (pkg != null && !pkg.isBlank()) {
                return "src/test/java/" + pkg.replace('.', '/') + "/" + cls;
            }
            // No package line: keep under src/test/java using the emitted relative path.
            if (lower.startsWith("src/test/java/")) return p;
            return "src/test/java/" + p;
        }
        return "src/test/resources/" + basename(p);
    }

    /** Put a batch of parsed test files into the map keyed by normalized path (later ones overwrite). */
    private void merge(Map<String, GeneratedFile> testFiles, List<GeneratedFile> batch) {
        for (GeneratedFile f : batch) {
            testFiles.put(normalizeTestPath(f), f);
        }
    }

    /** Extract the package name from a .java source string, or null if none. */
    private String extractPackage(String javaSource) {
        Matcher m = PACKAGE_DECL.matcher(javaSource);
        return m.find() ? m.group(1) : null;
    }

    /** Concatenate parsed files back into a single context string for the next call's prompt. */
    private String joinFiles(List<GeneratedFile> files) {
        StringBuilder sb = new StringBuilder();
        for (GeneratedFile f : files) {
            sb.append("### File: ").append(f.path()).append("\n")
                    .append(f.content()).append("\n\n");
        }
        return sb.toString();
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

    // ---- fixed test scaffolding (NOT LLM-generated) -------------------------

    /**
     * Ensure the generated project can compile + run tests: if pom.xml lacks spring-boot-starter-test
     * or H2, insert the test dependency block before </dependencies>. Also write a deterministic
     * src/test/resources/application-test.yml (H2 in-memory, create-drop) so integration tests with
     * @ActiveProfiles("test") run without an external Postgres. Idempotent.
     */
    void ensureTestScaffolding(File codeDir) {
        File pom = new File(codeDir, "pom.xml");
        if (pom.exists()) {
            try {
                String xml = Files.readString(pom.toPath());
                if (!xml.contains("spring-boot-starter-test")) {
                    int idx = xml.indexOf("</dependencies>");
                    if (idx > -1) {
                        String patched = xml.substring(0, idx) + testDepsXml() + "\n    " + xml.substring(idx);
                        Files.writeString(pom.toPath(), patched);
                        log.info("Injected test dependencies (spring-boot-starter-test + H2) into {}", pom);
                    }
                }
            } catch (IOException e) {
                log.warn("Could not patch pom.xml with test deps ({}): {}", pom, e.getMessage());
            }
        }
        // H2 test profile so `mvn test` is self-contained.
        File testRes = new File(codeDir, "src/test/resources");
        if (testRes.mkdirs() || testRes.exists()) {
            try {
                Files.writeString(new File(testRes, "application-test.yml").toPath(),
                        applicationTestYml(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
                log.info("Wrote deterministic application-test.yml (H2) into {}", testRes);
            } catch (IOException e) {
                log.warn("Could not write application-test.yml: {}", e.getMessage());
            }
        }
    }

    /** Test dependency block injected into a pom that predates the test-ready template. */
    private String testDepsXml() {
        return """
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
                """.trim();
    }

    /** H2 in-memory test profile. MODE=PostgreSQL so H2 accepts Postgres-flavored SQL. */
    private String applicationTestYml() {
        return """
                spring:
                  datasource:
                    url: jdbc:h2:mem:testdb;MODE=PostgreSQL;DB_CLOSE_DELAY=-1
                    driver-class-name: org.h2.Driver
                    username: sa
                    password:
                  jpa:
                    hibernate:
                      ddl-auto: create-drop
                    show-sql: false
                    properties:
                      hibernate:
                        dialect: org.hibernate.dialect.H2Dialect
                """.trim();
    }

    // ---- test-compile loop --------------------------------------------------

    /**
     * Self-correcting test-compile: ensure the Maven wrapper is present, run `mvn test-compile`,
     * and on failure feed the errors + current test files back to the LLM, overwrite the test
     * files it returns (filtered to real test classes), and retry. Up to MAX_COMPILE_ATTEMPTS times.
     * Returns true if it eventually compiled.
     */
    private boolean compileTestsWithSelfCorrection(File codeDir, Map<String, GeneratedFile> testFiles, String kb) {
        copyMavenWrapper(codeDir);

        for (int attempt = 1; attempt <= MAX_COMPILE_ATTEMPTS; attempt++) {
            CompileResult r = runMvnTestCompile(codeDir);
            log.info("Test-compile attempt {}/{}: exit={} ({} chars output)",
                    attempt, MAX_COMPILE_ATTEMPTS, r.exit, r.output.length());

            if (r.exit == 0) {
                log.info("Generated tests COMPILED on attempt {}", attempt);
                return true;
            }
            if (attempt == MAX_COMPILE_ATTEMPTS) {
                log.warn("Tests did not compile after {} attempts. Last errors:\n{}",
                        MAX_COMPILE_ATTEMPTS, truncate(r.output, 2000));
                return false;
            }

            log.warn("Test-compile attempt {} failed - asking LLM to fix. Errors:\n{}",
                    attempt, truncate(r.output, 1500));

            String fixedRaw = llmClient.chat(AgentType.TESTER,
                    fixSystemPrompt(kb),
                    fixUserPrompt(testFiles.values(), r.output),
                    3500);
            List<GeneratedFile> fixedFiles = parseFiles(fixedRaw);
            // Keep only real test classes so a stray "fixed" main source can't overwrite real code.
            fixedFiles = fixedFiles.stream()
                    .filter(f -> TEST_CLASS_NAME.matcher(normalizeTestPath(f)).find())
                    .toList();
            if (fixedFiles.isEmpty()) {
                log.warn("LLM returned no fixable TEST files on attempt {} - retrying compile as-is", attempt);
                continue;
            }
            for (GeneratedFile f : fixedFiles) {
                String normalized = normalizeTestPath(f);
                testFiles.put(normalized, f);
                storageService.save("jobs/" + jobId(codeDir) + "/code/" + normalized, f.content());
                log.info("LLM fix overwrote test {}", normalized);
            }
        }
        return false;
    }

    /** Run `mvnw -q test-compile` in the generated project directory. */
    private CompileResult runMvnTestCompile(File codeDir) {
        List<String> cmd = System.getProperty("os.name").toLowerCase().contains("win")
                ? List.of("cmd", "/c", "mvnw.cmd", "-q", "test-compile")
                : List.of("./mvnw", "-q", "test-compile");
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
     * Copy the platform's Maven wrapper into the generated project so it can build standalone.
     * Idempotent - the Developer Agent usually already copied it; this is a backstop for jobs
     * generated before the developer phase existed or run manually.
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
        } catch (IOException e) {
            log.warn("Could not copy Maven wrapper into generated project (test-compile will likely fail): {}", e.getMessage());
        }
    }

    private static void copyFile(File src, File dst) throws IOException {
        if (!src.exists()) return;
        Files.copy(src.toPath(), dst.toPath(), StandardCopyOption.REPLACE_EXISTING);
    }

    private String fixSystemPrompt(String kb) {
        return """
                You are a Senior QA Automation Engineer fixing TEST COMPILATION errors in a generated
                Spring Boot project's src/test/java test suite. Standards: %s
                You will receive the current TEST files and the `mvn test-compile` error output. Output
                corrected versions of ONLY the test files that need changes, each marked with a
                `### File: <path>` header followed by raw content. Use the SAME path the file was emitted
                with. Do NOT output unchanged files, main source, or prose, and no fences. Fix the root
                cause: wrong imports, mismatched method/field names, wrong Mockito/JUnit APIs, missing
                package declarations. Tests must match the ACTUAL main-source signatures.
                """.formatted(kb);
    }

    private String fixUserPrompt(java.util.Collection<GeneratedFile> files, String errors) {
        StringBuilder sb = new StringBuilder();
        sb.append("Test compile errors:\n").append(truncate(errors, 2000)).append("\n\n");
        sb.append("Current test files:\n");
        for (GeneratedFile f : files) {
            sb.append("### File: ").append(f.path()).append("\n")
                    .append(truncate(f.content(), 1500)).append("\n\n");
        }
        sb.append("Output corrected versions of the TEST files that need fixing, each marked `### File: <path>`.\n");
        sb.append("Use the SAME path as the original. Output ONLY test classes (name ending in Test/Tests/IT). No prose, no fences.\n");
        return sb.toString();
    }

    // ---- persistence --------------------------------------------------------

    private void saveArtifactRow(String jobId, GeneratedFile file, String normalizedPath, String storedPath) {
        PipelineJob job = pipelineJobRepository.findById(UUID.fromString(jobId)).orElse(null);
        Artifact artifact = Artifact.builder()
                .pipelineJob(job)
                .artifactType(ArtifactType.TEST_CODE)
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
        if (p.endsWith(".java")) return "text/x-java-source";
        if (p.endsWith(".yml") || p.endsWith(".yaml")) return "application/yaml";
        if (p.endsWith(".xml")) return "application/xml";
        return "text/plain";
    }

    // ---- small helpers ------------------------------------------------------

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

    /** Result of one `mvn test-compile` run. */
    private record CompileResult(int exit, String output, long millis) {}
}
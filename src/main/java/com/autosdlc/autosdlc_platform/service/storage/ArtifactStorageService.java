package com.autosdlc.autosdlc_platform.service.storage;


import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import java.io.IOException;
import java.nio.file.*;

@Service
@Slf4j
public class ArtifactStorageService {

    private final Path basePath = Paths.get("./artifacts");

    public String saveJson(String jobId, String subFolder, String fileName, String jsonContent) {
        try {
            Path filePath = basePath.resolve("jobs").resolve(jobId).resolve(subFolder).resolve(fileName);
            Files.createDirectories(filePath.getParent());
            Files.writeString(filePath, jsonContent, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            log.info("Saved artifact locally: {}", filePath.toAbsolutePath());
            // Later replace this return with MinIO path: jobs/{jobId}/...
            return filePath.toString();
        } catch (IOException e) {
            throw new RuntimeException("Failed to save artifact", e);
        }
    }

    /**
     * Save any text content by relative path under the artifact base dir
     * (e.g. "jobs/{jobId}/design/hld.md"). Returns the full path stored in the
     * artifacts table. Used by agents that produce non-JSON files (md, sql, yaml, html).
     */
    public String save(String relativePath, String content) {
        try {
            Path filePath = basePath.resolve(relativePath);
            Files.createDirectories(filePath.getParent());
            Files.writeString(filePath, content, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            log.info("Saved artifact locally: {}", filePath.toAbsolutePath());
            return filePath.toString();
        } catch (IOException e) {
            throw new RuntimeException("Failed to save artifact: " + relativePath, e);
        }
    }

    /**
     * Read text content back by relative path (e.g. "jobs/{jobId}/requirements/requirements.json").
     * Returns null if the file is missing - the caller decides what to do.
     * Used by downstream agents to read the previous agent's output.
     */
    public String read(String relativePath) {
        try {
            Path path = basePath.resolve(relativePath);
            if (!Files.exists(path)) {
                log.warn("Artifact not found on disk: {}", path);
                return null;
            }
            return Files.readString(path);
        } catch (IOException e) {
            log.error("Failed to read artifact: {}", relativePath, e);
            return null;
        }
    }

    /**
     * Save reviewer feedback for a specific agent under jobs/{jobId}/feedback/{agent}.txt.
     * Used by the approval gate: when a reviewer rejects an agent's output with modification
     * notes, the feedback is stored here so the agent can read + inject it on its re-run.
     */
    public void saveFeedback(String jobId, String agent, String feedback) {
        save("jobs/" + jobId + "/feedback/" + agent + ".txt", feedback == null ? "" : feedback);
    }

    /**
     * Read reviewer feedback for an agent, or null if none / empty. Agents call this at the
     * start of their run; a null return means a fresh run (no feedback to incorporate).
     */
    public String readFeedback(String jobId, String agent) {
        String s = read("jobs/" + jobId + "/feedback/" + agent + ".txt");
        return (s == null || s.isBlank()) ? null : s;
    }

    /** Clear feedback for an agent (called on approve so a later revise starts fresh). */
    public void clearFeedback(String jobId, String agent) {
        save("jobs/" + jobId + "/feedback/" + agent + ".txt", "");
    }
}
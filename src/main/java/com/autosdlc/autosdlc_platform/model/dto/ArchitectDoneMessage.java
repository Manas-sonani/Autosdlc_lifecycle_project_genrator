package com.autosdlc.autosdlc_platform.model.dto;

/**
 * Done message published to the architect-done queue when the Architect Agent
 * finishes. The orchestrator consumes it and hands off to the Developer Agent,
 * so this only needs to carry the jobId.
 */
public record ArchitectDoneMessage(String jobId) {}
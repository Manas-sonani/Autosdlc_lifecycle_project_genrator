package com.autosdlc.autosdlc_platform.model.dto;

/**
 * Done message published to the developer-done queue when the Developer Agent
 * finishes. The orchestrator consumes it and hands off to the Tester Agent,
 * so this only needs to carry the jobId.
 */
public record DeveloperDoneMessage(String jobId) {}
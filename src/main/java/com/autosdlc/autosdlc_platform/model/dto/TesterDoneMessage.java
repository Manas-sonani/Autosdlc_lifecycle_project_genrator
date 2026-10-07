package com.autosdlc.autosdlc_platform.model.dto;

/**
 * Done message published to the tester-done queue when the Tester Agent finishes.
 * The orchestrator consumes it and hands off to the DevOps Agent, so this only
 * needs to carry the jobId.
 */
public record TesterDoneMessage(String jobId) {}
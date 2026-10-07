package com.autosdlc.autosdlc_platform.model.dto;

/**
 * Task message published to the tester queue when the Developer completes.
 * The tester reads the generated code from jobs/{jobId}/code/ by jobId, so this
 * only needs to carry the jobId.
 */
public record TesterTaskMessage(String jobId) {}
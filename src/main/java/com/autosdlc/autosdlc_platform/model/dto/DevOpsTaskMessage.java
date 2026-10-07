package com.autosdlc.autosdlc_platform.model.dto;

/**
 * Task message published to the devops queue when the Tester completes. The DevOps
 * agent reads the generated code + design from storage by jobId, so this only
 * needs to carry the jobId.
 */
public record DevOpsTaskMessage(String jobId) {}
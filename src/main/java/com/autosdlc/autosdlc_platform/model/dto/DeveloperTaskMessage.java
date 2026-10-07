package com.autosdlc.autosdlc_platform.model.dto;

/**
 * Task message published to the developer queue when the Architect completes.
 * The developer reads the architect's design artifacts (schema.sql, api-spec.yaml,
 * lld.md) from storage by jobId, so this only needs to carry the jobId.
 */
public record DeveloperTaskMessage(String jobId) {}
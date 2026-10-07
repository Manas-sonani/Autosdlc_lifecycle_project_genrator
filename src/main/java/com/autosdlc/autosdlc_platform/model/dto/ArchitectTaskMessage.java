package com.autosdlc.autosdlc_platform.model.dto;

/**
 * Task message published to the architect queue when BA completes.
 * The architect reads BA's requirements.json from storage by jobId, so this
 * only needs to carry the jobId.
 */
public record ArchitectTaskMessage(String jobId) {}
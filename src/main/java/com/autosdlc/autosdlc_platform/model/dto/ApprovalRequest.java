package com.autosdlc.autosdlc_platform.model.dto;

/**
 * Request body for the human approval gate (POST /api/pipeline/{jobId}/approval).
 *
 * After each agent finishes, the pipeline pauses in AWAITING_APPROVAL. The reviewer then posts:
 *   - approved=true            -> advance to the next agent (or mark COMPLETED after DevOps).
 *   - approved=false, feedback -> re-run the SAME agent with this feedback injected into its
 *                                 LLM prompt, then pause for approval again.
 * feedback is required when approved=false; ignored when approved=true.
 */
public record ApprovalRequest(boolean approved, String feedback) {}
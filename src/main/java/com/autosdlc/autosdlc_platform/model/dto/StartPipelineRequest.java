package com.autosdlc.autosdlc_platform.model.dto;

import jakarta.validation.constraints.NotBlank;
import lombok.*;

@Getter @Setter @NoArgsConstructor @AllArgsConstructor @Builder
public class StartPipelineRequest {
    @NotBlank(message = "Project name is required")
    private String projectName;

    @NotBlank(message = "Description is required")
    @jakarta.validation.constraints.Size(min = 5, message = "Description must be at least 5 chars")
    private String description;

    /**
     * Optional: when true the pipeline runs end-to-end without pausing for human approval
     * after each agent (the "approve all" behavior). Defaults to false (gate pauses each stage).
     */
    private boolean autoApprove;
}
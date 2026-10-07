package com.autosdlc.autosdlc_platform.model.dto;


import com.autosdlc.autosdlc_platform.model.enums.PipelineStatus;
import lombok.*;
import java.time.LocalDateTime;
import java.util.UUID;

@Getter @Setter @NoArgsConstructor @AllArgsConstructor @Builder
public class PipelineResponse {
    private UUID jobId;
    private String projectName;
    private PipelineStatus status;
    private String currentStage;
    private String message;
    private LocalDateTime createdAt;}
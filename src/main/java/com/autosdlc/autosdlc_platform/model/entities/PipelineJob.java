//package com.autosdlc.autosdlc_platform.model.entities;
//import com.autosdlc.autosdlc_platform.model.enums.*;
//
//import com.autosdlc.autosdlc_platform.model.enums.PipelineStatus;
//import jakarta.persistence.*;
//import lombok.Builder;
//import org.hibernate.annotations.UuidGenerator;
//import java.time.LocalDateTime;
//import java.util.UUID;
//
//@Entity
//@Builder
//@Table(name = "pipeline_jobs")
//public class PipelineJob {
//
//    @Id
//    @GeneratedValue
//    @UuidGenerator
//    private UUID id;
//
//    @Column(nullable = false, columnDefinition = "TEXT")
//    private String userRequest;
//
//    @Enumerated(EnumType.STRING)
//    @Column(nullable = false)
//    private PipelineStatus status;
//
//    private String currentStage;
//    protected PipelineJob() {}
//
//    public String getUserRequest() {
//        return userRequest;
//    }
//
//    public void setUserRequest(String userRequest) {
//        this.userRequest = userRequest;
//    }
//
//    public UUID getId() {
//        return id;
//    }
//
//    public void setId(UUID id) {
//        this.id = id;
//    }
//
//    public PipelineStatus getStatus() {
//        return status;
//    }
//
//    public void setStatus(PipelineStatus status) {
//        this.status = status;
//    }
//
//    public LocalDateTime getCreatedAt() {
//        return createdAt;
//    }
//
//    public void setCreatedAt(LocalDateTime createdAt) {
//        this.createdAt = createdAt;
//    }
//
//    public String getCurrentStage() {
//        return currentStage;
//    }
//
//    public void setCurrentStage(String currentStage) {
//        this.currentStage = currentStage;
//    }
//
//    public LocalDateTime getCompletedAt() {
//        return completedAt;
//    }
//
//    public void setCompletedAt(LocalDateTime completedAt) {
//        this.completedAt = completedAt;
//    }
//
//    public String getErrorMessage() {
//        return errorMessage;
//    }
//
//    public void setErrorMessage(String errorMessage) {
//        this.errorMessage = errorMessage;
//    }
//
//    @Column(nullable = false)
//    private LocalDateTime createdAt;
//
//    private LocalDateTime completedAt;
//
//    @Column(columnDefinition = "TEXT")
//    private String errorMessage;
//
//
//}

package com.autosdlc.autosdlc_platform.model.entities;

import com.autosdlc.autosdlc_platform.model.enums.PipelineStatus;
import jakarta.persistence.*;
import lombok.Builder;
import org.hibernate.annotations.UuidGenerator;

import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Builder
@Table(name = "pipeline_jobs")
public class PipelineJob {

    @Id
    @GeneratedValue
    @UuidGenerator
    private UUID id;

    @Column(nullable = false, columnDefinition = "TEXT")
    private String userRequest;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private PipelineStatus status;

    private String currentStage;

    @Column(nullable = false)
    private LocalDateTime createdAt;

    private LocalDateTime completedAt;

    @Column(columnDefinition = "TEXT")
    private String errorMessage;

    /**
     * When true, the pipeline auto-advances through every agent without pausing for human
     * approval (the original end-to-end behavior). Set via StartPipelineRequest.autoApprove at
     * start time, or flipped on by POST /api/pipeline/{jobId}/approve-all ("approve all").
     * Default false -> the gate pauses after each agent.
     */
    private boolean autoApprove;

    /**
     * Required by Hibernate/JPA.
     */
    protected PipelineJob() {
    }

    /**
     * Constructor for creating PipelineJob manually.
     */
    public PipelineJob(
            UUID id,
            String userRequest,
            PipelineStatus status,
            String currentStage,
            LocalDateTime createdAt,
            LocalDateTime completedAt,
            String errorMessage,
            boolean autoApprove
    ) {
        this.id = id;
        this.userRequest = userRequest;
        this.status = status;
        this.currentStage = currentStage;
        this.createdAt = createdAt;
        this.completedAt = completedAt;
        this.errorMessage = errorMessage;
        this.autoApprove = autoApprove;
    }

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public String getUserRequest() {
        return userRequest;
    }

    public void setUserRequest(String userRequest) {
        this.userRequest = userRequest;
    }

    public PipelineStatus getStatus() {
        return status;
    }

    public void setStatus(PipelineStatus status) {
        this.status = status;
    }

    public String getCurrentStage() {
        return currentStage;
    }

    public void setCurrentStage(String currentStage) {
        this.currentStage = currentStage;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }

    public LocalDateTime getCompletedAt() {
        return completedAt;
    }

    public void setCompletedAt(LocalDateTime completedAt) {
        this.completedAt = completedAt;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public boolean isAutoApprove() {
        return autoApprove;
    }

    public void setAutoApprove(boolean autoApprove) {
        this.autoApprove = autoApprove;
    }
}
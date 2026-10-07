package com.autosdlc.autosdlc_platform.model.entities;
import com.autosdlc.autosdlc_platform.model.enums.*;

import com.autosdlc.autosdlc_platform.model.enums.ArtifactType;
import jakarta.persistence.*;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.time.LocalDateTime;

@Setter
@Getter
@Entity
@Builder
@Table(name = "artifacts")
public class Artifact {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "pipeline_job_id", nullable = false)
    private PipelineJob pipelineJob;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private ArtifactType artifactType;

    @Column(nullable = false)
    private String fileName;

    @Column(nullable = false)
    private String minioPath;

    private String contentType;

    private Long fileSize;

    @Column(nullable = false)
    private LocalDateTime createdAt;

    // Getters and Setters


}
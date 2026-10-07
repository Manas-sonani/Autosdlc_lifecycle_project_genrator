package com.autosdlc.autosdlc_platform.model.repository;


import com.autosdlc.autosdlc_platform.model.entities.Artifact;
import com.autosdlc.autosdlc_platform.model.enums.ArtifactType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface ArtifactRepository extends JpaRepository<Artifact, Long> {

    // === FREE from JpaRepository ===
    // save(artifact), findById(id), findAll(), deleteById(id)

    // === You need these for BA Agent + Dashboard ===

    // All files for one job: jobs/{jobId}/requirements/...
    List<Artifact> findByPipelineJobId(UUID jobId);

    // Get latest artifact of a type for a job
    Optional<Artifact> findByPipelineJobIdAndArtifactType(UUID jobId, ArtifactType artifactType);

    // All artifacts of a type across jobs
    List<Artifact> findByArtifactType(ArtifactType artifactType);

    // Latest artifact for a job (for dashboard)
    Optional<Artifact> findTopByPipelineJobIdOrderByCreatedAtDesc(UUID jobId);

    // Check if BA already generated requirements for this job
    boolean existsByPipelineJobIdAndArtifactType(UUID jobId, ArtifactType artifactType);
}
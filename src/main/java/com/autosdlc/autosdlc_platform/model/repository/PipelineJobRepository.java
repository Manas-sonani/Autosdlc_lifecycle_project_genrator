package com.autosdlc.autosdlc_platform.model.repository;


import com.autosdlc.autosdlc_platform.model.entities.PipelineJob;
import com.autosdlc.autosdlc_platform.model.enums.PipelineStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@Repository
public interface PipelineJobRepository extends JpaRepository<PipelineJob, UUID> {

    // === These you get for FREE from JpaRepository ===
    // save(PipelineJob job) - Create or Update
    // findById(UUID id) - returns Optional<PipelineJob>
    // findAll() - returns List<PipelineJob>
    // deleteById(UUID id)
    // count()
    // existsById(UUID id)

    // === Custom methods you will need for Orchestrator & Dashboard ===

    // Find all jobs in a specific status
    List<PipelineJob> findByStatus(PipelineStatus status);

    // Find jobs that are still running or pending
    List<PipelineJob> findByStatusIn(List<PipelineStatus> statuses);

    // Find by current agent stage
    List<PipelineJob> findByCurrentStage(String currentStage);

    // For dashboard - latest 10 jobs
    List<PipelineJob> findTop10ByOrderByCreatedAtDesc();

    // For cleanup / monitoring - jobs created after a time
    List<PipelineJob> findByCreatedAtAfter(LocalDateTime dateTime);

    // For stats
    long countByStatus(PipelineStatus status);
}
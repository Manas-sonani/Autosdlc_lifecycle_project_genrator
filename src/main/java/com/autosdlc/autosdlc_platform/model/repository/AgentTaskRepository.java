

package com.autosdlc.autosdlc_platform.model.repository;

import com.autosdlc.autosdlc_platform.model.entities.AgentTask;
import com.autosdlc.autosdlc_platform.model.enums.AgentType;
import com.autosdlc.autosdlc_platform.model.enums.PipelineStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface AgentTaskRepository extends JpaRepository<AgentTask, Long> {

    // Free from JpaRepository: save(), findById(), findAll(), deleteById()

    // Get all tasks for one job
    List<AgentTask> findByJobId(String jobId);

    // Get BA task for a job
    Optional<AgentTask> findByJobIdAndAgentType(String jobId, AgentType agentType);

    // Check pending tasks
    List<AgentTask> findByStatus(PipelineStatus status);

    // Latest task for a job
    Optional<AgentTask> findTopByJobIdOrderByStartedAtDesc(String jobId);
}
package com.autosdlc.autosdlc_platform.service.knowledgebase;


import com.autosdlc.autosdlc_platform.model.dto.StandardDTO;
import com.autosdlc.autosdlc_platform.model.entities.Standard;
import com.autosdlc.autosdlc_platform.model.enums.AgentType;
import com.autosdlc.autosdlc_platform.model.repository.StandardRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import java.util.*;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class KnowledgeBaseService {

    private final StandardRepository standardRepository;

    public List<Standard> findByCategory(String category) {
        return standardRepository.findByCategory(category.toLowerCase());
    }

    public List<Standard> searchByKeyword(String keyword) {
        return standardRepository.searchByKeyword(keyword);
    }

    public List<StandardDTO> getAllAsDto() {
        return standardRepository.findAll().stream()
                .map(this::toDto)
                .collect(Collectors.toList());
    }

    /**
     * MOST IMPORTANT METHOD for BA Agent
     * Builds a formatted string of relevant standards to inject into LLM prompt
     */
    public String getContextForAgent(AgentType agentType, String taskKeyword) {
        List<Standard> relevantStandards = new ArrayList<>();

        // 1. Map Agent to categories - this is your guardrail logic
        switch (agentType) {
            case BUSINESS_ANALYST:
                relevantStandards.addAll(standardRepository.findByCategoryIn(
                        List.of("requirements", "naming", "api")));
                break;
            case DESIGNER:
                relevantStandards.addAll(standardRepository.findByCategoryIn(
                        List.of("api", "database", "naming", "security")));
                break;
            case DEVELOPER:
                relevantStandards.addAll(standardRepository.findByCategoryIn(
                        List.of("coding", "naming", "database", "security", "api")));
                break;
            case TESTER:
                relevantStandards.addAll(standardRepository.findByCategory("testing"));
                break;
            case DEVOPS:
                relevantStandards.addAll(standardRepository.findByCategoryIn(
                        List.of("security", "coding")));
                break;
        }

        // 2. Add keyword based search if task provided
        if (taskKeyword != null && !taskKeyword.isBlank()) {
            relevantStandards.addAll(standardRepository.searchByKeyword(taskKeyword));
        }

        // Remove duplicates
        relevantStandards = relevantStandards.stream()
                .collect(Collectors.toMap(Standard::getId, s -> s, (a, b) -> a))
                .values().stream().toList();

        if (relevantStandards.isEmpty()) {
            return "No specific standards found. Follow general best practices.";
        }

        // 3. Format for LLM prompt
        StringBuilder context = new StringBuilder();
        context.append("=== COMPANY STANDARDS YOU MUST FOLLOW ===\n");
        for (Standard s : relevantStandards) {
            context.append(String.format("\n[%s - %s]\n%s\nTags: %s\n",
                    s.getCategory().toUpperCase(), s.getTitle(), s.getContent(), s.getTags()));
        }
        context.append("\n=== END OF STANDARDS ===\n");

        log.info("Built context for agent {} with {} standards", agentType, relevantStandards.size());
        return context.toString();
    }

    private StandardDTO toDto(Standard entity) {
        return StandardDTO.builder()
                .id(entity.getId())
                .category(entity.getCategory())
                .title(entity.getTitle())
                .content(entity.getContent())
                .tags(entity.getTags())
                .build();
    }
}
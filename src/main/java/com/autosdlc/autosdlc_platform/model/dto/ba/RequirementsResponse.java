package com.autosdlc.autosdlc_platform.model.dto.ba;



import lombok.*;
import java.util.List;

@Getter @Setter @NoArgsConstructor @AllArgsConstructor @Builder
public class RequirementsResponse {
    private String projectName;
    private String projectDescription;
    private List<FunctionalReq> functionalRequirements;
    private List<NonFunctionalReq> nonFunctionalRequirements;
    private List<UserStory> userStories;
    private List<UseCase> useCases;

    @Getter @Setter @NoArgsConstructor @AllArgsConstructor @Builder
    public static class FunctionalReq {
        private String id; // REQ-001
        private String title;
        private String description;
        private String priority; // MUST, SHOULD, COULD
        private String acceptanceCriteria; // Given-When-Then
    }
    @Getter @Setter @NoArgsConstructor @AllArgsConstructor @Builder
    public static class NonFunctionalReq {
        private String id;
        private String category; // performance, security, scalability
        private String description;
    }
    @Getter @Setter @NoArgsConstructor @AllArgsConstructor @Builder
    public static class UserStory {
        private String id; // US-001
        private String role;
        private String feature;
        private String benefit;
        private List<String> acceptanceCriteria;
    }
    @Getter @Setter @NoArgsConstructor @AllArgsConstructor @Builder
    public static class UseCase {
        private String id;
        private String actor;
        private String title;
        private List<String> steps;
    }
}

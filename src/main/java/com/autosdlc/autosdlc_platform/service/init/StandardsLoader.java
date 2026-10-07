package com.autosdlc.autosdlc_platform.service.init;


import com.autosdlc.autosdlc_platform.model.entities.Standard;
import com.autosdlc.autosdlc_platform.model.repository.StandardRepository;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;
import java.util.List;

@Component
@RequiredArgsConstructor
@Slf4j
public class StandardsLoader implements CommandLineRunner {

    private final StandardRepository standardRepository;

    private final ObjectMapper objectMapper; // Spring Boot auto-provides this


    @Override
    public void run(String... args) throws Exception {
        long count = standardRepository.count();
        if (count > 0) {
            log.info("Standards table already has {} entries, skipping loader", count);
            return;
        }

        log.info("Standards table empty, loading from coding-standards.json...");

        try {
            ClassPathResource resource = new ClassPathResource("standards/coding-standards.json");
            List<Standard> standards = objectMapper.readValue(
                    resource.getInputStream(),
                    new TypeReference<List<Standard>>() {}
            );

            standardRepository.saveAll(standards);
            log.info("Successfully loaded {} standards into DB", standards.size());

        } catch (Exception e) {
            log.error("Failed to load standards", e);
            throw e;
        }
    }
}
package com.autosdlc.autosdlc_platform.model.repository;

import com.autosdlc.autosdlc_platform.model.entities.Standard;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;
import java.util.List;

@Repository
public interface StandardRepository extends JpaRepository<Standard, Long> {

    List<Standard> findByCategory(String category);

    List<Standard> findByCategoryIn(List<String> categories);

    List<Standard> findByTagsContainingIgnoreCase(String keyword);

    // Full text search on title + content + tags - very useful for BA agent
    @Query("SELECT s FROM Standard s WHERE LOWER(s.title) LIKE LOWER(CONCAT('%', :keyword, '%')) " +
            "OR LOWER(s.content) LIKE LOWER(CONCAT('%', :keyword, '%')) " +
            "OR LOWER(s.tags) LIKE LOWER(CONCAT('%', :keyword, '%'))")
    List<Standard> searchByKeyword(String keyword);
}
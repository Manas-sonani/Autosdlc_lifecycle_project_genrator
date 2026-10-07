package com.autosdlc.autosdlc_platform.model.dto;


import lombok.*;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class StandardDTO {
    private Long id;
    private String category;
    private String title;
    private String content;
    private String tags;
}
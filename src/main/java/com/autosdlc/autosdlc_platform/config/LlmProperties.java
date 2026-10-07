package com.autosdlc.autosdlc_platform.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Externalised LLM provider pool + per-agent fallback chains.
 *
 * Providers: any number of named blocks under llm.providers.&lt;name&gt;.*. Each has a
 *   type      - "openai" (OpenAI-compatible /v1/chat/completions) or "ollama" (native /api/chat)
 *   base-url  - full chat endpoint URL
 *   api-key   - bearer token (blank for local Ollama, which ignores auth)
 *   model     - model id the provider understands
 *
 * Agent chains: llm.agents.&lt;AgentType&gt;=name1,name2,... tried in order. If a provider
 * returns 429 (rate limit / quota) or 5xx, the call automatically falls back to the next.
 * Add a new API by dropping a provider block here and adding its name to a chain - no code change.
 *
 * Example:
 *   llm.providers.groq1.type=openai
 *   llm.providers.groq1.base-url=https://api.groq.com/openai/v1/chat/completions
 *   llm.providers.groq1.api-key=gsk_xxx
 *   llm.providers.groq1.model=openai/gpt-oss-120b
 *   llm.agents.BUSINESS_ANALYST=groq1,groq2,openrouter
 */
@Getter
@Setter
@ConfigurationProperties(prefix = "llm")
public class LlmProperties {

    /** Named provider pool. Key = provider name used in agent chains. */
    private Map<String, Provider> providers = new LinkedHashMap<>();

    /** Per-agent ordered fallback chain. Key = AgentType.name() (e.g. BUSINESS_ANALYST). */
    private Map<String, List<String>> agents = new LinkedHashMap<>();

    @Getter
    @Setter
    public static class Provider {
        /** "openai" (OpenAI-compatible) or "ollama" (native /api/chat). Defaults to openai. */
        private String type = "openai";
        private String baseUrl;
        private String apiKey;
        private String model;
    }
}
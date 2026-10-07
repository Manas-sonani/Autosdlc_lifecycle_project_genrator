package com.autosdlc.autosdlc_platform.service.llm;


import com.autosdlc.autosdlc_platform.config.LlmProperties;
import com.autosdlc.autosdlc_platform.model.enums.AgentType;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestTemplate;

import java.util.*;

/**
 * LLM gateway backed by a configurable provider pool (see {@link LlmProperties}).
 *
 * Each agent has an ordered fallback chain of providers. A call tries the first provider;
 * if it returns 429 (rate limit / quota exhausted) or 5xx, the call automatically falls back
 * to the next provider in the chain. Only if EVERY provider fails does the call throw.
 *
 * This is what lets us "not run out of tokens": spread load across multiple Groq free-tier
 * keys / other OpenAI-compatible providers, with the local Ollama model as an unlimited backstop.
 *
 * Two provider shapes are supported via the provider's "type":
 *   openai - OpenAI-compatible /v1/chat/completions  (Groq, OpenRouter, Together, etc.)
 *   ollama - Ollama native /api/chat with think:false (keeps qwen3.5 reasoning off so it
 *            doesn't eat the token budget; content is always clean)
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class LLMClient {

    private final LlmProperties llmProperties;
    private final RestTemplate restTemplate = new RestTemplate();

    /**
     * Call the LLM for the given agent, walking its configured provider chain.
     * Falls back to the next provider on 429 (rate/quota) or 5xx. Throws only if all fail.
     */
    public String chat(AgentType agent, String systemPrompt, String userPrompt, int maxTokens) {
        List<String> chain = resolveChain(agent);
        RuntimeException lastError = null;

        for (String name : chain) {
            LlmProperties.Provider provider = llmProperties.getProviders().get(name);
            if (provider == null) {
                log.warn("Provider '{}' referenced by agent {} is not configured - skipping", name, agent);
                continue;
            }
            try {
                return callProvider(provider, systemPrompt, userPrompt, maxTokens);
            } catch (HttpClientErrorException.TooManyRequests e) {
                // 429 = per-minute rate limit OR daily quota exhausted -> next provider.
                log.warn("Provider '{}' for {} returned 429 (rate/quota) - falling back to next", name, agent);
                lastError = e;
            } catch (HttpServerErrorException e) {
                log.warn("Provider '{}' for {} returned {} - falling back to next", name, agent, e.getStatusCode());
                lastError = e;
            }
        }
        throw new RuntimeException(
                "All LLM providers failed for agent " + agent + " (tried " + chain + ")", lastError);
    }

    /** Convenience overload with a default max_tokens of 2000. */
    public String chat(AgentType agent, String systemPrompt, String userPrompt) {
        return chat(agent, systemPrompt, userPrompt, 2000);
    }

    /** Resolve the ordered provider chain for an agent, falling back to all providers if unset. */
    private List<String> resolveChain(AgentType agent) {
        List<String> chain = llmProperties.getAgents().get(agent.name());
        if (chain == null || chain.isEmpty()) {
            chain = new ArrayList<>(llmProperties.getProviders().keySet());
            if (chain.isEmpty()) {
                throw new IllegalStateException(
                        "No LLM providers configured under llm.providers.* and no chain for agent " + agent);
            }
            log.warn("No provider chain configured for agent {} - using all providers: {}", agent, chain);
        }
        return chain;
    }

    private String callProvider(LlmProperties.Provider p, String systemPrompt, String userPrompt, int maxTokens) {
        if ("ollama".equalsIgnoreCase(p.getType())) {
            return callOllamaNative(p, systemPrompt, userPrompt, maxTokens);
        }
        return callOpenAiCompatible(p, systemPrompt, userPrompt, maxTokens);
    }

    /** OpenAI-compatible /chat/completions path - Groq, OpenRouter, Together, etc. */
    @SuppressWarnings("unchecked")
    private String callOpenAiCompatible(LlmProperties.Provider p, String systemPrompt,
                                        String userPrompt, int maxTokens) {
        var requestBody = Map.of(
                "model", p.getModel(),
                "messages", List.of(
                        Map.of("role", "system", "content", systemPrompt),
                        Map.of("role", "user", "content", userPrompt)
                ),
                "temperature", 0.2,
                "max_tokens", maxTokens
        );

        var headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        if (p.getApiKey() != null && !p.getApiKey().isBlank()) {
            headers.setBearerAuth(p.getApiKey());
        }

        log.info("Calling LLM [openai | {}]: model={} max_tokens={}", p.getBaseUrl(), p.getModel(), maxTokens);
        var response = restTemplate.postForEntity(
                p.getBaseUrl(), new HttpEntity<>(requestBody, headers), Map.class
        );

        var choices = (List<Map<String, Object>>) response.getBody().get("choices");
        return (String) ((Map<String, Object>) choices.get(0).get("message")).get("content");
    }

    /**
     * Ollama native /api/chat path. Uses think:false so qwen3.5 reasoning is off (it would
     * otherwise eat the token budget and truncate output). Ollama puts any reasoning in a
     * separate message.thinking field, so message.content is always clean.
     */
    @SuppressWarnings("unchecked")
    private String callOllamaNative(LlmProperties.Provider p, String systemPrompt,
                                    String userPrompt, int maxTokens) {
        var requestBody = Map.of(
                "model", p.getModel(),
                "messages", List.of(
                        Map.of("role", "system", "content", systemPrompt),
                        Map.of("role", "user", "content", userPrompt)
                ),
                "stream", false,
                "think", false,
                // num_predict = max tokens to generate; temperature = sampling.
                "options", Map.of("num_predict", maxTokens, "temperature", 0.2)
        );

        var headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        if (p.getApiKey() != null && !p.getApiKey().isBlank()) {
            headers.setBearerAuth(p.getApiKey());
        }

        log.info("Calling LLM [ollama | {}]: model={} num_predict={}", p.getBaseUrl(), p.getModel(), maxTokens);
        var response = restTemplate.postForEntity(
                p.getBaseUrl(), new HttpEntity<>(requestBody, headers), Map.class
        );

        Map<String, Object> message = (Map<String, Object>) response.getBody().get("message");
        return (String) message.get("content");
    }
}
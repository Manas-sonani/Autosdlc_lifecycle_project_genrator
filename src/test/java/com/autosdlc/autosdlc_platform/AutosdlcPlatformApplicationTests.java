package com.autosdlc.autosdlc_platform;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * Smoke test - verifies the Spring application context loads without errors.
 * (Requires Postgres + RabbitMQ to be reachable, since @SpringBootTest boots
 * the full context.)
 */
@SpringBootTest
class AutosdlcPlatformApplicationTests {

	@Test
	void contextLoads() {
		// If the context fails to start, this test fails with the root cause.
	}
}
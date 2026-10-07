package com.autosdlc.autosdlc_platform.config;

import org.springframework.amqp.core.*;
import org.springframework.amqp.support.converter.JacksonJsonMessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitMQConfig {
    public static final String BA_QUEUE = "agent.ba.queue";
    public static final String ARCH_QUEUE = "agent.architect.queue";
    public static final String DEV_QUEUE = "agent.developer.queue";
    public static final String TEST_QUEUE = "agent.tester.queue";
    public static final String DEVOPS_QUEUE = "agent.devops.queue";
    public static final String ORCH_QUEUE = "orchestrator.queue";
    public static final String ARCH_DONE_QUEUE = "orchestrator.architect.done.queue";
    public static final String DEV_DONE_QUEUE = "orchestrator.developer.done.queue";
    public static final String TEST_DONE_QUEUE = "orchestrator.tester.done.queue";
    public static final String EXCHANGE = "autosdlc.exchange";
    public static final String BA_KEY = "ba.task";
    public static final String ARCH_KEY = "architect.task";
    public static final String DEV_KEY = "developer.task";
    public static final String TEST_KEY = "tester.task";
    public static final String DEVOPS_KEY = "devops.task";
    public static final String ORCH_KEY = "orchestrator.done";
    public static final String ARCH_DONE_KEY = "architect.done";
    public static final String DEV_DONE_KEY = "developer.done";
    public static final String TEST_DONE_KEY = "tester.done";

    @Bean public Queue baQueue() { return new Queue(BA_QUEUE, true); }
    @Bean public Queue archQueue() { return new Queue(ARCH_QUEUE, true); }
    @Bean public Queue devQueue() { return new Queue(DEV_QUEUE, true); }
    @Bean public Queue testQueue() { return new Queue(TEST_QUEUE, true); }
    @Bean public Queue devopsQueue() { return new Queue(DEVOPS_QUEUE, true); }
    @Bean public Queue orchQueue() { return new Queue(ORCH_QUEUE, true); }
    @Bean public Queue archDoneQueue() { return new Queue(ARCH_DONE_QUEUE, true); }
    @Bean public Queue devDoneQueue() { return new Queue(DEV_DONE_QUEUE, true); }
    @Bean public Queue testDoneQueue() { return new Queue(TEST_DONE_QUEUE, true); }
    @Bean public DirectExchange exchange() { return new DirectExchange(EXCHANGE); }
    @Bean public Binding baBinding() { return BindingBuilder.bind(baQueue()).to(exchange()).with(BA_KEY); }
    @Bean public Binding archBinding() { return BindingBuilder.bind(archQueue()).to(exchange()).with(ARCH_KEY); }
    @Bean public Binding devBinding() { return BindingBuilder.bind(devQueue()).to(exchange()).with(DEV_KEY); }
    @Bean public Binding testBinding() { return BindingBuilder.bind(testQueue()).to(exchange()).with(TEST_KEY); }
    @Bean public Binding devopsBinding() { return BindingBuilder.bind(devopsQueue()).to(exchange()).with(DEVOPS_KEY); }
    @Bean public Binding orchBinding() { return BindingBuilder.bind(orchQueue()).to(exchange()).with(ORCH_KEY); }
    @Bean public Binding archDoneBinding() { return BindingBuilder.bind(archDoneQueue()).to(exchange()).with(ARCH_DONE_KEY); }
    @Bean public Binding devDoneBinding() { return BindingBuilder.bind(devDoneQueue()).to(exchange()).with(DEV_DONE_KEY); }
    @Bean public Binding testDoneBinding() { return BindingBuilder.bind(testDoneQueue()).to(exchange()).with(TEST_DONE_KEY); }
    @Bean public JacksonJsonMessageConverter converter() { return new JacksonJsonMessageConverter(); }
    @Bean
    public org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(
            org.springframework.amqp.rabbit.connection.ConnectionFactory cf,
            JacksonJsonMessageConverter converter) {
        var factory = new org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(cf);
        factory.setMessageConverter(converter);
        factory.setDefaultRequeueRejected(false); // <-- critical: don't re-queue on error
        return factory;
    }
}
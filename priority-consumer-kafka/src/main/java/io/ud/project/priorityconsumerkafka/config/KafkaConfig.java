package io.ud.project.priorityconsumerkafka.config;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.HashMap;
import java.util.Map;

@Configuration
@SuppressWarnings("unused")
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public NewTopic p1() {
        return TopicBuilder.name("Daily").build();
    }

    @Bean
    public NewTopic p2() {
        return TopicBuilder.name("Weekly").build();
    }

    @Bean
    public NewTopic p3() {
        return TopicBuilder.name("Monthly").build();
    }

    @Bean
    public KafkaAdmin admin() {
        Map<String, Object> adminProperties = new HashMap<>();
        adminProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return new KafkaAdmin(adminProperties);
    }
}

package io.ud.project.priorityconsumerkafka.consumer;

import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import java.util.function.Consumer;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.util.*;

@Component
@Slf4j
public class PriorityBasedConsumer {

    @Value("${spring.kafka.consumer.group-id}")
    private String consumerGroupId;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    private final List<TopicConsumer> consumersInPriorityOrder = new ArrayList<>();

    @RequiredArgsConstructor(staticName = "of")
    @Getter
    private static class TopicConsumer {
        private final String topic;
        private final KafkaConsumer<String, String> kafkaConsumer;
        private final Consumer<ConsumerRecords<String, String>> consumerLogic;
    }

    private void highPriorityConsumer(ConsumerRecords<String, String> records) {
        // high priority processing...
    }

    private void mediumPriorityConsumer(ConsumerRecords<String, String> records) {
        // medium priority processing...
    }

    private void lowPriorityConsumer(ConsumerRecords<String, String> records) {
        // low priority processing...
    }

    @PostConstruct
    public void init() {
        Map<String, Consumer<ConsumerRecords<String, String>>> topicVsConsumerLogic = new HashMap<>();
        topicVsConsumerLogic.put("high_priority_queue", this::highPriorityConsumer);
        topicVsConsumerLogic.put("medium_priority_queue", this::mediumPriorityConsumer);
        topicVsConsumerLogic.put("low_priority_queue", this::lowPriorityConsumer);
        // if you're taking the topic names from external configuration, make sure to order it based on priority.
        for (String topic : Arrays.asList("high_priority_queue", "medium_priority_queue", "low_priority_queue")) {
            Properties consumerProperties = new Properties();
            consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
            // add other properties.
            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties);
            consumer.subscribe(Collections.singletonList(topic));
            consumersInPriorityOrder.add(TopicConsumer.of(topic, consumer, topicVsConsumerLogic.get(topic)));
        }
    }

    @EventListener(ApplicationReadyEvent.class) // To execute once the application is ready.
    @SneakyThrows
    public void startConsumers() {
        // For illustration purposes, I just wrote this synchronous code. Use thread pools where ever
        // necessary for high performance.
        while (true) { // poll infinitely
            try {
                // Consumers iterated based on priority.
                for (TopicConsumer topicConsumer : consumersInPriorityOrder) {
                    ConsumerRecords<String, String> records
                            = topicConsumer.getKafkaConsumer().poll(Duration.ofMillis(100));
                    if (!records.isEmpty()) {
                        topicConsumer.getConsumerLogic().accept(records);
                        break;  // To start consuming again based on priority.
                    }
                }
            } catch (Exception e) {
                // on any unknown runtime exceptions, ignoring here. You can add your proper logic.
                log.error("Unknown exception occurred.", e);
            }
        }
    }
}

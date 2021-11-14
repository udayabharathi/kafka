package io.ud.project.priorityconsumerkafka.consumer;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationContext;
import org.springframework.context.event.EventListener;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

@Component
@RequiredArgsConstructor
@SuppressWarnings("unused")
@Slf4j
public class PriorityBasedConsumer {

    private final Environment environment;

    private final ApplicationContext context;

    @Value("${spring.kafka.consumer.group-id}")
    private String consumerGroupId;

    @Value("${kafka.topic.to.consume.topics}")
    private List<String> topics;

    @Value("${kafka.topic.consumer.threads}")
    private Integer consumerThreads;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    private List<SortedMap<String, ConsumerDTO>> topicVsKafkaConsumersPerThread;
    private Map<String, Consumer> topicVsConsumerLogic;
    private ExecutorService executor;
    private AtomicInteger totalConsumed;

    protected static final List<String> CONSUMED = new ArrayList<>();

    @PostConstruct
    public void init() {
        totalConsumed = new AtomicInteger(0);
        Map<String, Integer> topicVsPriority = getTopicVsPriority();
        topicVsKafkaConsumersPerThread = new ArrayList<>();
        topicVsConsumerLogic = new HashMap<>();
        for (int i = 0; i < consumerThreads; i++) {
            SortedMap<String, ConsumerDTO> topicVsKafkaConsumers = new TreeMap<>((topic1, topic2) ->
                    topicVsPriority.getOrDefault(topic2, 1) - topicVsPriority.getOrDefault(topic1, 1));
            for (String topic : topics) {
                Properties consumerProperties = new Properties();
                consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
                consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
                consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
                consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
                consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
                KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties);
                ConsumerDTO consumerDTO = ConsumerDTO.of(consumer, new HashMap<>());
                consumer.subscribe(Collections.singletonList(topic), consumerDTO);
                topicVsKafkaConsumers.put(topic, consumerDTO);
                topicVsConsumerLogic.put(topic, getConsumer(topic));
            }
            topicVsKafkaConsumersPerThread.add(topicVsKafkaConsumers);
        }
        executor = Executors.newFixedThreadPool(consumerThreads);
    }

    @EventListener(ApplicationReadyEvent.class)
    @SneakyThrows
    public void startConsumers() {
        Thread.sleep(100);
        for (SortedMap<String, ConsumerDTO> topicVsKafkaConsumers : topicVsKafkaConsumersPerThread) {
            executor.submit(() -> consume(topicVsKafkaConsumers));
        }
    }

    private void consume(SortedMap<String, ConsumerDTO> topicVsKafkaConsumers) {
        try {
            while (totalConsumed.intValue() < 50) {
                for (Map.Entry<String, ConsumerDTO> topicVsConsumer : topicVsKafkaConsumers.entrySet()) {
                    ConsumerRecords<String, String> records
                            = topicVsConsumer.getValue().getConsumer().poll(Duration.of(100, ChronoUnit.MILLIS));
                    log.info("topic : {}, consumed: {}, totalConsumed: {}", topicVsConsumer.getKey(), records.count(), totalConsumed);
                    if (!records.isEmpty()) {
                        totalConsumed.updateAndGet(v -> v + records.count());
                        records.forEach(consumerRecord -> topicVsConsumer.getValue().getOffsets().put(new TopicPartition(consumerRecord.topic(), consumerRecord.partition()), new OffsetAndMetadata(consumerRecord.offset() + 1, null)));
                        topicVsConsumer.getValue().getConsumer().commitAsync(topicVsConsumer.getValue().getOffsets(), null);
                        topicVsConsumerLogic.getOrDefault(topicVsConsumer.getKey(), Consumer.DEFAULT).consume(records);
                        break;
                    }
                }
            }
            log.info("***********************");
            for (String data : CONSUMED)
                System.out.println(data);
            log.info("***********************");
        } finally {
            if (!CollectionUtils.isEmpty(topicVsKafkaConsumersPerThread))
                topicVsKafkaConsumers.values().forEach(value -> value.getConsumer().close());
        }
    }

    private Map<String, Integer> getTopicVsPriority() {
        Map<String, Integer> topicVsPriority = new HashMap<>();
        for (String topic : topics) {
            topicVsPriority.put(topic, environment.getProperty("kafka.topic."+topic+".priority", Integer.class, 1));
        }
        return topicVsPriority;
    }

    private Consumer getConsumer(String topic) {
        try {
            return context.getBean("consumer-" + topic, Consumer.class);
        } catch (BeansException e) {
            log.error("No consumer found for topic: {}", topic, e);
            return Consumer.DEFAULT;
        }
    }
    @RequiredArgsConstructor(staticName = "of")
    @Getter
    private static class ConsumerDTO implements ConsumerRebalanceListener {  // 4
        private final KafkaConsumer<String, String> consumer;
        private final Map<TopicPartition, OffsetAndMetadata> offsets;
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> collection) {
            consumer.commitSync(offsets);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> collection) {
            // do nothing
        }

        @Override
        public void onPartitionsLost(Collection<TopicPartition> partitions) {
            ConsumerRebalanceListener.super.onPartitionsLost(partitions);
        }
    }
}

package io.ud.project.priorityconsumerkafka.consumer;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationContext;
import org.springframework.context.event.EventListener;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;

@Component
@RequiredArgsConstructor
@SuppressWarnings("unused")
@Slf4j
public class PriorityBasedConsumer {

    private final Environment environment;

    private final ThreadPoolTaskExecutor executor;

    private final ApplicationContext context;

    @Value("${spring.zookeeper.connect}")
    private String zookeeperConnect;

    @Value("${spring.kafka.consumer.group-id}")
    private String consumerGroupId;

    @Value("${kafka.topic.to.consume.topics}")
    private List<String> topics;

    @Value("${kafka.topic.consumer.threads}")
    private Integer consumerThreads;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    private SortedMap<String, KafkaConsumer<String, String>> topicVsKafkaConsumers;
    private Map<String, Consumer> topicVsConsumerLogic;

    public static final List<String> CONSUMED = new ArrayList<>();

    @PostConstruct
    public void init() {
        Map<String, Integer> topicVsPriority = getTopicVsPriority();
        topicVsKafkaConsumers = new TreeMap<>((topic1, topic2) ->
                topicVsPriority.getOrDefault(topic2, 1) - topicVsPriority.getOrDefault(topic1, 1));
        topicVsConsumerLogic = new HashMap<>();
        for (String topic : topics) {
            Properties consumerProperties = new Properties();
            consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
            consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties);
            consumer.subscribe(Collections.singletonList(topic));
            topicVsKafkaConsumers.put(topic, consumer);
            topicVsConsumerLogic.put(topic, getConsumer(topic));
        }
    }

    @EventListener(ApplicationReadyEvent.class)
    @SneakyThrows
    public void startConsumers() {
        int totalConsumed = 0;
        Thread.sleep(100);
        try {
            while (totalConsumed < 50) {
                for (Map.Entry<String, KafkaConsumer<String, String>> topicVsConsumer : topicVsKafkaConsumers.entrySet()) {
                    ConsumerRecords<String, String> records
                            = topicVsConsumer.getValue().poll(Duration.of(100, ChronoUnit.MILLIS));
                    log.info("topic : {}, consumed: {}, totalConsumed: {}", topicVsConsumer.getKey(), records.count(), totalConsumed);
                    if (!records.isEmpty()) {
                        totalConsumed += records.count();
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
            if (!CollectionUtils.isEmpty(topicVsKafkaConsumers))
                topicVsKafkaConsumers.values().forEach(KafkaConsumer::close);
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
}

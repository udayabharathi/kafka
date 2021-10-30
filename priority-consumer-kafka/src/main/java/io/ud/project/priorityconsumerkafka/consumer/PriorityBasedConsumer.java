package io.ud.project.priorityconsumerkafka.consumer;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationContext;
import org.springframework.context.event.EventListener;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.*;

@Component
@RequiredArgsConstructor
@SuppressWarnings("unused")
@Slf4j
public class PriorityBasedConsumer {

    private final Environment environment;

    private final ThreadPoolTaskExecutor executor;

    private final ApplicationContext context;

    private ConsumerConnector consumerConnector;

    @Value("${spring.zookeeper.connect}")
    private String zookeeperConnect;

    @Value("${spring.kafka.consumer.group-id}")
    private String consumerGroupId;

    @Value("${kafka.topic.to.consume.topics}")
    private List<String> topics;

    @Value("${kafka.topic.consumer.threads}")
    private Integer consumerThreads;

    @PostConstruct
    public void init() {
        Properties consumerProperties = new Properties();
        consumerProperties.put("zookeeper.connect", zookeeperConnect);
        consumerProperties.put("group.id", consumerGroupId);
        consumerProperties.put("zookeeper.session.timeout.ms", "400");
        consumerProperties.put("zookeeper.sync.time.ms", "200");
        consumerProperties.put("auto.commit.interval.ms", "1000");
        consumerProperties.put("auto.offset.reset", "smallest");
        ConsumerConfig consumerConfig = new ConsumerConfig(consumerProperties);
        consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
    }

    @RequiredArgsConstructor(staticName = "of")
    @Getter
    private static class ConsumerStreamPair {
        private final io.ud.project.priorityconsumerkafka.consumer.Consumer consumer;
        private final KafkaStream<byte[], byte[]> stream;
    }

    @RequiredArgsConstructor(staticName = "of")
    private static class ConsumerThread implements Runnable {
        private final SortedMap<Integer, ConsumerStreamPair> prioritizedConsumerStreamPairMap;
        @SneakyThrows
        @Override
        public void run() {
            while (true) {
                try {
                    log.info("Executing fetch!");
                    getNextMessageAvailableStream()
                            .ifPresent(consumerStreamPair ->
                                    consumerStreamPair.getConsumer()
                                            .consume(consumerStreamPair.getStream().iterator().next()));
                    Thread.sleep(100);
                } catch (Exception e) {
                    log.error("Exception occurred.", e);
                }
            }
        }
        private Optional<ConsumerStreamPair> getNextMessageAvailableStream() {
            for (Map.Entry<Integer, ConsumerStreamPair> priorityVsConsumerStreamPair : prioritizedConsumerStreamPairMap.entrySet()) {
                if (priorityVsConsumerStreamPair.getValue().getStream().iterator().hasNext())
                    return Optional.of(priorityVsConsumerStreamPair.getValue());
            }
            return Optional.empty();
        }
    }

    @EventListener(ApplicationReadyEvent.class)
    public void startConsumers() {
        Map<String, Integer> topicConsumerMap = new HashMap<>();
        for (String topic : topics)
            topicConsumerMap.put(topic, consumerThreads);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicConsumerMap);
        List<SortedMap<Integer, ConsumerStreamPair>> constructedPrioritizedMapList = constructPrioritizedMapList(consumerMap);
        for (SortedMap<Integer, ConsumerStreamPair> prioritizedMap : constructedPrioritizedMapList) {
            executor.submit(ConsumerThread.of(prioritizedMap));
        }
    }

    private List<SortedMap<Integer, ConsumerStreamPair>> constructPrioritizedMapList(Map<String, List<KafkaStream<byte[], byte[]>>> topicVsStreams) {
        List<SortedMap<Integer, ConsumerStreamPair>> prioritizedMapList = new ArrayList<>();
        for (int i = 0; i < consumerThreads; i++) {
            SortedMap<Integer, ConsumerStreamPair> prioritizedMap = new TreeMap<>((a, b) -> (b - a));
            for (String topic : topics) {
                Integer topicPriority = getPriority(topic);
                io.ud.project.priorityconsumerkafka.consumer.Consumer consumer = getConsumer(topic);
                prioritizedMap.put(topicPriority, ConsumerStreamPair.of(consumer, topicVsStreams.get(topic).get(i)));
            }
            prioritizedMapList.add(prioritizedMap);
        }
        return prioritizedMapList;
    }

    private Integer getPriority(String topicName) {
        return environment.getProperty("kafka.topic." + topicName + ".priority", Integer.class, 0);
    }

    private io.ud.project.priorityconsumerkafka.consumer.Consumer getConsumer(String topic) {
        try {
            return context.getBean("consumer-" + topic, io.ud.project.priorityconsumerkafka.consumer.Consumer.class);
        } catch (BeansException e) {
            log.error("Unable to get the consumer object for topic: {}.", topic, e);
            return message -> log.info("No consumer available for topic: {}. Message: [{}] is ignored", topic, message.message());
        }
    }
}

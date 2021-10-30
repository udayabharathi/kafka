package io.ud.project.priorityconsumerkafka.consumer;

import com.riferrei.kafka.core.BucketPriorityAssignor;
import com.riferrei.kafka.core.BucketPriorityConfig;
import io.ud.project.priorityconsumerkafka.PriorityConsumerKafkaApplication;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

@Component
@SuppressWarnings("unused")
@Slf4j
public class Consumer {

    private class ConsumerThread extends Thread {

        private String threadName;
        private KafkaConsumer<String, String> consumer;

        public ConsumerThread(String bucketName,
                              String threadName, Properties configs) {

            this.threadName = threadName;

            configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class.getName());

            configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class.getName());

            configs.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
            configs.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "reports-group");

            // Implementing the bucket priority pattern

            configs.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                    BucketPriorityAssignor.class.getName());

            configs.put(BucketPriorityConfig.TOPIC_CONFIG, "reports");
            configs.put(BucketPriorityConfig.BUCKETS_CONFIG, "Daily, Weekly, Monthly");
            configs.put(BucketPriorityConfig.ALLOCATION_CONFIG, "70%, 15%, 15%");
            configs.put(BucketPriorityConfig.BUCKET_CONFIG, bucketName);

            consumer = new KafkaConsumer<>(configs);
            consumer.subscribe(Arrays.asList("reports"));

        }

        @Override
        public void run() {
            for (;;) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofSeconds(Integer.MAX_VALUE));
                for (ConsumerRecord<String, String> record : records) {
                    log.info(String.format("[%s] Key = %s, Partition = %d",
                            threadName, record.key(), record.partition()));
                }
            }
        }

    }

    private final List<ConsumerThread> consumerThreads = new ArrayList<>();

    @EventListener(ApplicationReadyEvent.class)
    @SneakyThrows
    public void run() {
        Thread.sleep(30000);
        String[] buckets = new String[] {"Daily", "Weekly", "Monthly"};
        for (int i = 0; i < 6; i++) {
            String threadName = String.format("%s-Thread-%d", "reports", i);
            consumerThreads.add(new ConsumerThread(buckets[i%3], threadName, PriorityConsumerKafkaApplication.getConfigs()));
        }
        consumerThreads.stream().forEach(ct -> ct.start());
    }
}

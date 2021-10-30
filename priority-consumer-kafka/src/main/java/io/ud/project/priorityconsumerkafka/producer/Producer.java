package io.ud.project.priorityconsumerkafka.producer;

import com.riferrei.kafka.core.BucketPriorityConfig;
import com.riferrei.kafka.core.BucketPriorityPartitioner;
import com.riferrei.kafka.core.DiscardPartitioner;
import io.ud.project.priorityconsumerkafka.PriorityConsumerKafkaApplication;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

@Component
@SuppressWarnings("unused")
@Slf4j
public class Producer {

    @EventListener(ApplicationReadyEvent.class)
    public void run() {
        Properties configs = PriorityConsumerKafkaApplication.getConfigs();
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        // Implementing the bucket priority pattern

        configs.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,
                BucketPriorityPartitioner.class.getName());

        configs.put(BucketPriorityConfig.TOPIC_CONFIG, "reports");
        configs.put(BucketPriorityConfig.BUCKETS_CONFIG, "Daily, Monthly, Weekly");
        configs.put(BucketPriorityConfig.ALLOCATION_CONFIG, "70%, 15%, 15%");
        configs.put(BucketPriorityConfig.FALLBACK_PARTITIONER_CONFIG,
                DiscardPartitioner.class.getName());
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(configs)) {

            AtomicInteger counter = new AtomicInteger(0);
            String[] buckets = {"Daily", "Monthly", "Weekly"};

            for (;;) {

                int value = counter.incrementAndGet();

                final String recordKey = buckets[value % 5 == 0 ? 0 : (value % 2) + 1] + "-" + value;

                ProducerRecord<String, String> record =
                        new ProducerRecord<>("reports", recordKey, "Value: "+recordKey);

                producer.send(record, (metadata, exception) -> {
                    System.out.println(String.format(
                            "Key '%s' was sent to partition %d",
                            recordKey, metadata == null ? null : metadata.partition()));
                });

                try {
                    Thread.sleep(100);
                } catch (InterruptedException ie) {
                }

            }

        }
    }
}

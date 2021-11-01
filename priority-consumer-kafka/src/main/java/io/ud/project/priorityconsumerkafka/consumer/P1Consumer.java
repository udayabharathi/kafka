package io.ud.project.priorityconsumerkafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.stereotype.Component;

@Slf4j
@Component("consumer-Daily")
@SuppressWarnings("unused")
public class P1Consumer implements Consumer {
    @Override
    public void consume(ConsumerRecords<String, String> records) {
        records.forEach(consumerRecord -> {
            log.info("Consumer:[{}] Received: {}", this.getClass().getName(), consumerRecord.toString());
            PriorityBasedConsumer.CONSUMED.add(consumerRecord.value());
        });
    }
}

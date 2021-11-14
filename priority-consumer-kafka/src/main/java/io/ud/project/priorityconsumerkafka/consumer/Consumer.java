package io.ud.project.priorityconsumerkafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface Consumer {

    Consumer DEFAULT = new Consumer() {};

    Logger LOGGER = LoggerFactory.getLogger(Consumer.class);

    default void consume(ConsumerRecords<String, String> records) {
        LOGGER.error("No consumer logic found. Dropping records : {}", records);
    }
}

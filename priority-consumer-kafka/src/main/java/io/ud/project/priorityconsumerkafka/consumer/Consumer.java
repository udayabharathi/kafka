package io.ud.project.priorityconsumerkafka.consumer;

import kafka.message.MessageAndMetadata;

public interface Consumer {
    void consume(MessageAndMetadata<byte[], byte[]> data);
}

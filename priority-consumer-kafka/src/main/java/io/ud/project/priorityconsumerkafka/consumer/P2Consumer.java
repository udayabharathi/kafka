package io.ud.project.priorityconsumerkafka.consumer;

import kafka.message.MessageAndMetadata;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component("consumer-priority-2")
@SuppressWarnings("unused")
public class P2Consumer implements Consumer {
    @Override
    public void consume(MessageAndMetadata<byte[], byte[]> data) {
        log.info("Consumer:[{}] Received: {}", this.getClass().getName(), new String(data.message()));
    }
}

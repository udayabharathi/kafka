package io.ud.project.priorityconsumerkafka.producer;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.Random;

@Component
@SuppressWarnings("unused")
@RequiredArgsConstructor
@Slf4j
public class RandomDataPublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ThreadPoolTaskExecutor executor;

    @EventListener(ApplicationReadyEvent.class)
    @SneakyThrows
    public void publishRandomMessage() {
        for (int i = 0; i < 3; i++) {
            executor.submit(Producer.of(kafkaTemplate, i));
        }
    }

    @RequiredArgsConstructor(staticName = "of")
    private static class Producer implements Runnable {

        private final KafkaTemplate<String, String> kafkaTemplate;
        private final int producerNumber;

        @Override
        @SneakyThrows
        public void run() {
            int count = 0;
            while (count++ < 1_000_000) {
                constructMessage(randomNumber(1, 10));
                Thread.sleep(1000);
            }
        }

        private void constructMessage(int number) {
            if (number == 1) {
                sendMessage("This is top most priority "+ LocalDateTime.now(), "priority-1");
            } else if (number > 1 && number < 5) {
                sendMessage("This is medium priority "+ LocalDateTime.now(), "priority-2");
            } else {
                sendMessage("This is lowest priority "+ LocalDateTime.now(), "priority-3");
            }
        }

        private void sendMessage(String message, String topic) {
            log.info("Producer-{}: Sending {} to {}", producerNumber, message, topic);
            kafkaTemplate.send(topic, message);
        }

        @SuppressWarnings("SameParameterValue")
        private static int randomNumber(int min, int max) {
            return new Random().nextInt((max - min) + 1) + min;
        }
    }


}

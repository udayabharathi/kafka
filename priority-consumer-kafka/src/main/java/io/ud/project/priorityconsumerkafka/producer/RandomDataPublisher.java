package io.ud.project.priorityconsumerkafka.producer;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

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
        for (int i = 0; i < 1; i++) {
            executor.submit(Producer.of(kafkaTemplate, i));
        }
    }

    @RequiredArgsConstructor(staticName = "of")
    private static class Producer implements Runnable {

        private final KafkaTemplate<String, String> kafkaTemplate;
        private final int producerNumber;
        private final List<String> publishedOrder = new ArrayList<>();

        @Override
        @SneakyThrows
        public void run() {
            int count = 0;
            while (count++ < 50) {
                constructMessage(randomNumber(1, 10));
            }
            log.info("***********************");
            for (String data : publishedOrder)
                System.out.println(data);
            log.info("***********************");
        }

        private void constructMessage(int number) {
            if (number == 1) {
                sendMessage("This is top most priority "+ UUID.randomUUID(), "Daily");
            } else if (number > 1 && number < 5) {
                sendMessage("This is medium priority "+ UUID.randomUUID(), "Weekly");
            } else {
                sendMessage("This is lowest priority "+ UUID.randomUUID(), "Monthly");
            }
        }

        private void sendMessage(String message, String topic) {
            log.info("Producer-{}: Sending {} to {}", producerNumber, message, topic);
            publishedOrder.add(message);
            kafkaTemplate.send(topic, message);
        }

        @SuppressWarnings("SameParameterValue")
        private static int randomNumber(int min, int max) {
            return new Random().nextInt((max - min) + 1) + min;
        }
    }


}

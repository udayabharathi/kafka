package io.ud.project.priorityconsumerkafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

@SpringBootApplication
public class PriorityConsumerKafkaApplication {

    public static void main(String[] args) {
        SpringApplication.run(PriorityConsumerKafkaApplication.class, args);
    }


    public static void createTopic(String topic, int numPartitions, short replicationFactor) {
        Properties configs = getConfigs();
        try (AdminClient adminClient = AdminClient.create(configs)) {
            ListTopicsResult listTopics = adminClient.listTopics();
            Set<String> existingTopics = listTopics.names().get();
            List<NewTopic> topicsToCreate = new ArrayList<>();
            if (!existingTopics.contains(topic)) {
                topicsToCreate.add(new NewTopic(topic,
                        numPartitions, replicationFactor));
            }
            adminClient.createTopics(topicsToCreate);
        } catch (InterruptedException | ExecutionException ex) {
        }
    }

    private static Properties configs = new Properties();

    static {
        try {
            try (InputStream is = PriorityConsumerKafkaApplication.class.getResourceAsStream("/application.properties")) {
                configs.load(is);
            }
        } catch (IOException ioe) {
        }
        createTopic("reports", 6, (short) 3);
    }

    public static Properties getConfigs() {
        return configs;
    }
}

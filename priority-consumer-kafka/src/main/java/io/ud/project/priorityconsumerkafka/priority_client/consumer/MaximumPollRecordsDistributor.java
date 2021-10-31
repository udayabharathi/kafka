package io.ud.project.priorityconsumerkafka.priority_client.consumer;

import com.flipkart.priority.kafka.client.consumer.burst.CapacityBurstPriorityKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Map;

/**
 * Provides distribution logic of maxPollRecords across priorities.
 * <br><br>
 * This is in the context of varying processing rates across priorities.
 * {@link CapacityBurstPriorityKafkaConsumer} uses {@link ConsumerConfig#MAX_POLL_RECORDS_CONFIG}
 * as the processing capacity parameter for tuning processing rates across priorities.
 */
@SuppressWarnings("unused")
public interface MaximumPollRecordsDistributor {

    /**
     * Distribute maxPollRecords across all priorities (upto maxPriority {exclusive}).
     *
     * @param maxPriority Max priority levels
     * @param maxPollRecords Max poll records
     * @return Mapping of maxPollRecords for each priority from [0, maxPriority - 1].
     * These must add upto the input maxPollRecords.
     */
    Map<Integer, Integer> distribution(int maxPriority, int maxPollRecords);
}

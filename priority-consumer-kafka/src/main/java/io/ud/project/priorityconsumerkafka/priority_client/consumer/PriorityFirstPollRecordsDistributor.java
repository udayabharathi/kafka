package io.ud.project.priorityconsumerkafka.priority_client.consumer;

import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("unused")
public class PriorityFirstPollRecordsDistributor implements MaximumPollRecordsDistributor {
    @Override
    public Map<Integer, Integer> distribution(int maxPriority, int maxPollRecords) {
        Map<Integer, Integer> response = new HashMap<>();
        response.put(0, maxPollRecords);
        for (int i = 1; i < maxPriority; i++) {
            response.put(i, 0);
        }
        return response;
    }
}

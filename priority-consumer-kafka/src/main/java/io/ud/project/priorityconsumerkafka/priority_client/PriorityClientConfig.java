package io.ud.project.priorityconsumerkafka.priority_client;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
@SuppressWarnings("unused")
public final class PriorityClientConfig {
    // Common
    public static final String MAX_PRIORITY_CONFIG = "max.priority";
    // Consumer
    public static final String MAX_POLL_HISTORY_WINDOW_SIZE_CONFIG = "max.poll.history.window.size";
    public static final String MIN_POLL_WINDOW_MAXOUT_THRESHOLD_CONFIG = "min.poll.window.maxout.threshold";
    public static final int DEFAULT_MAX_POLL_HISTORY_WINDOW_SIZE = 6;
    public static final int DEFAULT_MIN_POLL_WINDOW_MAXOUT_SIZE = 4;
}

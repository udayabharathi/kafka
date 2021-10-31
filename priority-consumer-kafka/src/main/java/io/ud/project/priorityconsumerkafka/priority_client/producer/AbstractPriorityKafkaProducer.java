package io.ud.project.priorityconsumerkafka.priority_client.producer;


import com.flipkart.priority.kafka.client.ClientConfigs;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Abstract producer implementation for priority producers.
 * <br><br>
 * Implementations should follow the below instructions:
 * <ul>
 *     <li>
 *         Implementations should whitelist functionality and by default no operation is allowed.
 *     </li>
 *     <li>
 *         When we discuss about priority topic XYZ, here XYZ is the logical name.
 *         For every logical priority topic XYZ one must define max supported priority level via
 *         the config {@link ClientConfigs#MAX_PRIORITY_CONFIG} property.
 *     </li>
 *     <li>
 *         Priorities are ordered as follows: <code>0 &lt; 1 &lt; 2 ... &lt; ${max.priority - 1}</code>.
 *     </li>
 *     <li>
 *         Exposes methods {@link #send(int, ProducerRecord)} and {@link #send(int, ProducerRecord, Callback)}
 *         to produce messages to a specific priority level topic. If {@link #send(ProducerRecord)} or
 *         {@link #send(ProducerRecord, Callback)} are used, then messages are defaulted to lowest
 *         priority level 0.
 *     </li>
 * </ul>
 */
@SuppressWarnings("unused")
public abstract class AbstractPriorityKafkaProducer<K, V> implements Producer<K, V> {

    public abstract int getMaxPriority();

    public abstract Future<RecordMetadata> send(int priority, ProducerRecord<K, V> producerRecord);

    public abstract Future<RecordMetadata> send(int priority, ProducerRecord<K, V> producerRecord, Callback callback);

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> producerRecord) {
        return send(0, producerRecord);
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> producerRecord, Callback callback) {
        return send(0, producerRecord, callback);
    }

    @Override
    public void flush() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException();
    }
}

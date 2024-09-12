package in.rgukt.r081247.kafka.consumer;

import in.rgukt.r081247.kafka.config.KafkaConsumerConfig;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class KafkaConsumerTask {
    Logger log = LoggerFactory.getLogger(KafkaConsumerTask.class);

    private String clientId;
    private final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
    private KafkaConsumer<String, Object> consumer;
    private Map<String, Object> props;
    private String topic;
    public KafkaConsumerTask(String clientId, Map<String, Object> props, String topic) {
        this.clientId = clientId;
        this.props = props;
        this.topic = topic;
    }

    public void init() {
        props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        consumer = new KafkaConsumer<>(props);
        Duration timeout = Duration.ofMillis(100);

        try {
            consumer.subscribe(Collections.singletonList(topic), new HandleRebalance());
            while (true) {
                ConsumerRecords<String, Object> records = consumer.poll(timeout);
                //log.info(clientId + " poll returned records: " + records.count());
                for (ConsumerRecord<String, Object> record : records) {
                    System.out.printf(clientId + ": topic = %s, partition = %d, offset = %d, key = %s, value = %s\n",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value());
                    currentOffsets.put(
                            new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1, null));
                }
                consumer.commitAsync(currentOffsets, null);
            }
        } catch (WakeupException e) {
            // ignore, we are closing
        } catch (Exception ex) {
            log.error(clientId + " Unexpected error", ex);
        } finally {
            try {
                consumer.commitSync(currentOffsets);
            } finally {
                consumer.close();
                System.out.println(clientId + " Closed consumer and we are done");
            }
        }
    }

    private class HandleRebalance implements ConsumerRebalanceListener {
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        }

        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            System.out.println(clientId + " Lost partitions in rebalance. " + "Committing current offsets:" + currentOffsets);
            consumer.commitSync(currentOffsets);
            for(TopicPartition partition: partitions)
                currentOffsets.remove(partition);
            System.out.println(clientId + " Lost partitions in rebalance. " + "offsets after rebalance:" + currentOffsets);
        }
    }

}

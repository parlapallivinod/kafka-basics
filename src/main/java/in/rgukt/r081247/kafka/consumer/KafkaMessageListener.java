package in.rgukt.r081247.kafka.consumer;

import in.rgukt.r081247.kafka.config.KafkaConsumerConfig;
import in.rgukt.r081247.kafka.dto.Customer;
import jakarta.annotation.PostConstruct;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Service
public class KafkaMessageListener {
    private ExecutorService executors = Executors.newFixedThreadPool(3);

    @Value("${kafka.topic}")
    private String topic;

    @Value("${kafka.group.id}")
    private String groupId;

    private final static Logger log = LoggerFactory.getLogger(KafkaMessageListener.class);

    @Autowired
    KafkaConsumerConfig consumerConfig;

    /*
    @KafkaListener(topics = "${kafka.topic}", groupId="jt-group")
    public void consumeEvents(Customer customer) {
        log.info("consumer consumed the events {}", customer.toString());
    }
     */

    @PostConstruct
    public void init() {
        log.info("inside init()");
        for(int i = 1; i < 4; i++) {
            final String consumerClientId = groupId + "-" +  i;
            Callable<Void> task = new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    KafkaConsumerTask task = new KafkaConsumerTask(consumerClientId,
                            consumerConfig.consumerConfig(), topic);
                    task.init();
                    return null;
                }
            };
            executors.submit(task);
        }
    }

    /*
    public void consumeEventsKafkaCore() {
        log.info("inside consumeKafka " + topic);
        KafkaConsumer<String, Object> consumer = new KafkaConsumer<>(consumerConfig.consumerConfig());
        Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
        Duration timeout = Duration.ofMillis(5000);

        class HandleRebalance implements ConsumerRebalanceListener {
            public void onPartitionsAssigned(Collection<TopicPartition>
                                                     partitions) {
            }

            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                System.out.println("Lost partitions in rebalance. " +
                        "Committing current offsets:" + currentOffsets);
                consumer.commitSync(currentOffsets);
                for(TopicPartition partition: partitions)
                    currentOffsets.remove(partition);
                System.out.println("Lost partitions in rebalance. " +
                        "offsets after rebalance:" + currentOffsets);
            }
        }

        try {
            consumer.subscribe(Collections.singletonList(topic), new HandleRebalance());
            while (true) {
                ConsumerRecords<String, Object> records = consumer.poll(timeout);
                log.info("poll returned records: " + records.count());
                for (ConsumerRecord<String, Object> record : records) {
                    System.out.printf("topic = %s, partition = %d, offset = %d, key = %s, value = %s\n",
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
            log.error("Unexpected error", ex);
        } finally {
            try {
                consumer.commitSync(currentOffsets);
            } finally {
                consumer.close();
                System.out.println("Closed consumer and we are done");
            }
        }
    }
     */
}

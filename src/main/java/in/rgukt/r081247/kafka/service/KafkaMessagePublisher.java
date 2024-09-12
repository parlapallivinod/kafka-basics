package in.rgukt.r081247.kafka.service;

import in.rgukt.r081247.kafka.dto.Customer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class KafkaMessagePublisher {
    @Value("${kafka.topic}")
    private String topic;

    /*
    @Autowired
    private KafkaTemplate<String, Object> template;
    */

    @Autowired
    @Qualifier("producer1")
    private KafkaProducer<String, Object> producer1;

    @Autowired
    @Qualifier("producer2")
    private KafkaProducer<String, Object> producer2;

    /*
    public void sentEventsToTopic(Customer customer) {
        try {
            CompletableFuture<SendResult<String, Object>> response = template.send(topic, customer);
            response.whenComplete((result, ex) -> {
                if(ex == null) {
                    System.out.println("Sent message=[" + customer.toString() + "] with offset=["
                            + result.getRecordMetadata().offset() + "]");
                } else {
                    System.out.println("Unable to send message=[" + customer.toString() + "] due to : " + ex.getMessage());
                }
            });
        } catch (Exception ex) {
            System.out.println("ERROR: " + ex.getMessage());
        }
    }

     */

    public void sendEventsToTopicKakaCore1(Customer customer) {
        ProducerRecord<String, Object> record = new ProducerRecord<>(topic, null, customer);
        producer1.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if(exception == null) {
                    System.out.println("producer 1 Sent message=[" + customer.toString() + "] with offset=["
                            + metadata.offset() + "] topic=[" + metadata.topic() + "] partition [" + metadata.partition() + "]");
                } else {
                    System.out.println("producer 2 Unable to send message=[" + customer.toString() + "] due to : " + exception.getMessage());
                }
            }
        });
    }

    public void sendEventsToTopicKakaCore2(Customer customer) {
        ProducerRecord<String, Object> record = new ProducerRecord<>(topic, null, customer);
        producer2.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if(exception == null) {
                    System.out.println("producer 2 Sent message=[" + customer.toString() + "] with offset=["
                            + metadata.offset() + "] topic=[" + metadata.topic() + "] partition [" + metadata.partition() + "]");

                } else {
                    System.out.println("producer 2 Unable to send message=[" + customer.toString() + "] due to : " + exception.getMessage());
                }
            }
        });
    }

}

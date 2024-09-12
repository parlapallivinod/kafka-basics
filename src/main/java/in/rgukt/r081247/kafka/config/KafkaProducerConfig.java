package in.rgukt.r081247.kafka.config;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig {
    @Value("${kafka.bootstrapserver}")
    private String bootstrapServer;

    @Value("${kafka.acks}")
    private String acks;

    @Value("${kafka.enable.idempotence}")
    private String enableIdempotence;

    @Value("${kafka.client.id}")
    private String clientId;

    @Bean
    @Scope("prototype")
    public Map<String, Object> producerConfig() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, acks);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, enableIdempotence);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        return props;
    }

    /*
    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfig());
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
    */

    @Bean("producer1")
    public KafkaProducer<String, Object> kafkaProducer1() {
        Map<String, Object> props = producerConfig();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "vinod-producer-1");
        return new KafkaProducer<>(props);
    }

    @Bean("producer2")
    public KafkaProducer<String, Object> kafkaProducer2() {
        Map<String, Object> props = producerConfig();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "vinod-producer-2");
        return new KafkaProducer<>(props);
    }

}

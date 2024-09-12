package in.rgukt.r081247.kafka.controller;

import in.rgukt.r081247.kafka.service.KafkaMessagePublisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import in.rgukt.r081247.kafka.dto.Customer;

@RestController
@RequestMapping("/producer-app")
public class EventController {
    @Autowired
    private KafkaMessagePublisher publisher;

    /*
    @PostMapping("/publish")
    public void sendEvents(@RequestBody Customer customer) {
        publisher.sentEventsToTopic(customer);
    }
     */

    @PostMapping("/publishkafka1")
    public void sendEventsKafkaCore1(@RequestBody Customer customer) {
        for(int i=0; i < 1000; i++)
            publisher.sendEventsToTopicKakaCore1(customer);
    }

    @PostMapping("/publishkafka2")
    public void sendEventsKafkaCore2(@RequestBody Customer customer) {
        for(int i=0; i < 1000; i++)
            publisher.sendEventsToTopicKakaCore2(customer);
    }

}

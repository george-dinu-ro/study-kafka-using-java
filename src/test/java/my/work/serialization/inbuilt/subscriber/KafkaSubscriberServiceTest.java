package my.work.serialization.inbuilt.subscriber;

import my.work.serialization.inbuilt.subscriber.KafkaSubscriberService;
import org.junit.jupiter.api.Test;
import my.work.serialization.inbuilt.publisher.KafkaPublisherService;

import static org.junit.jupiter.api.Assertions.assertEquals;

class KafkaSubscriberServiceTest {

    @Test
    void whenCallSubscribe_thenGetAllMessages() {
        var topic = "test-subscriber-topic-ds";
        var key = "laptop";
        var value = 10;

        var kafkaPublisherService = new KafkaPublisherService();
        var kafkaSubscriberService = new KafkaSubscriberService();

        kafkaPublisherService.publishWithoutResponse(topic, key, value);
        var message = kafkaSubscriberService.subscribe(topic);

        var currentMessage = message.iterator().next();

        assertEquals(topic, currentMessage.topic());

        assertEquals(key, currentMessage.key());

        assertEquals(value, currentMessage.value());
    }

}
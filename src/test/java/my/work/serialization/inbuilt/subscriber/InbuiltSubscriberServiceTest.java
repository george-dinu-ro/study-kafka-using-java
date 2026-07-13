package my.work.serialization.inbuilt.subscriber;

import org.junit.jupiter.api.Test;
import my.work.serialization.inbuilt.publisher.InbuiltPublisherService;

import static org.junit.jupiter.api.Assertions.assertEquals;

class InbuiltSubscriberServiceTest {

    @Test
    void whenCallSubscribe_thenGetAllMessages() {
        var topic = "test-subscriber-topic-ds";
        var key = "laptop";
        var value = 10;

        var kafkaPublisherService = new InbuiltPublisherService();
        var kafkaSubscriberService = new InbuiltSubscriberService();

        kafkaPublisherService.publishWithoutResponse(topic, key, value);
        var message = kafkaSubscriberService.subscribe(topic);

        var currentMessage = message.iterator().next();

        assertEquals(topic, currentMessage.topic());

        assertEquals(key, currentMessage.key());

        assertEquals(value, currentMessage.value());
    }

}
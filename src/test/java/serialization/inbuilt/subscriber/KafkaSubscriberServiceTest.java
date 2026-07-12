package serialization.inbuilt.subscriber;

import serialization.inbuilt.message.KafkaProductMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import serialization.inbuilt.publisher.KafkaPublisherService;

import static org.junit.jupiter.api.Assertions.assertEquals;

class KafkaSubscriberServiceTest {

    private static final String TOPIC = "test-consumer-topic";

    private static final String KEY = "laptop";

    private static final Integer VALUE = 10;

    private KafkaPublisherService kafkaPublisherService;

    private KafkaSubscriberService kafkaSubscriberService;

    private KafkaProductMessage message;

    @BeforeEach
    void beforeEach() {
        this.kafkaPublisherService = new KafkaPublisherService();
        this.kafkaSubscriberService = new KafkaSubscriberService();
        message = new KafkaProductMessage(TOPIC, KEY, VALUE);
    }

    @Test
    void whenCallSubscribe_thenGetAllMessages() {
        this.kafkaPublisherService.publishWithoutResponse(this.message);
        var actual = this.kafkaSubscriberService.subscribe(TOPIC);

        var currentMessage = actual.iterator().next();

        assertEquals(1, actual.count());

        assertEquals(TOPIC, currentMessage.topic());
    }

}
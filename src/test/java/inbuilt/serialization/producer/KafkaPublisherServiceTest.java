package inbuilt.serialization.producer;

import inbuilt.serialization.message.KafkaProductMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaPublisherServiceTest {

    private static final String TOPIC = "test-producer-topic";

    private static final String KEY = "pc";

    private static final Integer VALUE = 5;

    private KafkaPublisherService kafkaPublisherService;

    private KafkaProductMessage message;

    @BeforeEach
    void beforeEach() {
        kafkaPublisherService = new KafkaPublisherService();
        message = new KafkaProductMessage(TOPIC, KEY, VALUE);
    }

    @Test
    void whenCallPublishWithoutResponse_thenShouldReturnStaticSuccessMessage() {
        var actual = this.kafkaPublisherService.publishWithoutResponse(this.message);

        assertEquals("Message successfully sent to topic: %s".formatted(TOPIC), actual);
    }

    @Test
    void whenCallPublishWithFutureResponse_thenShouldReturnDynamicSuccessMessage() {
        var actual = this.kafkaPublisherService.publishWithFutureResponse(this.message);

        assertTrue(actual.startsWith("Message successfully sent to topic: %s".formatted(TOPIC)));
    }

    @Test
    void whenCallSendMessageWithCallbackResponse_thenShouldReturnDynamicSuccessMessage() throws InterruptedException {
        var callback = new ResponseMessageCallback();
        this.kafkaPublisherService.publishWithAsyncResponse(this.message, callback);
        var actual = callback.getMessage();

        assertTrue(actual.startsWith("Message successfully sent to topic: %s".formatted(TOPIC)));
    }

}
package serialization.inbuilt.publisher;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaPublisherServiceTest {

    private static final String TOPIC = "test-publisher-topic-ds";

    private static final String KEY = "pc";

    private static final Integer VALUE = 5;

    private KafkaPublisherService kafkaPublisherService;

    @BeforeEach
    void beforeEach() {
        kafkaPublisherService = new KafkaPublisherService();
    }

    @Test
    void whenCallPublishWithoutResponse_thenShouldReturnStaticSuccessMessage() {
        var actual = this.kafkaPublisherService.publishWithoutResponse(TOPIC, KEY, VALUE);

        assertEquals("Message successfully sent to topic: %s".formatted(TOPIC), actual);
    }

    @Test
    void whenCallPublishWithFutureResponse_thenShouldReturnDynamicSuccessMessage() {
        var actual = this.kafkaPublisherService.publishWithFutureResponse(TOPIC, KEY, VALUE);

        assertTrue(actual.startsWith("Message successfully sent to topic: %s".formatted(TOPIC)));
    }

    @Test
    void whenCallSendMessageWithCallbackResponse_thenShouldReturnDynamicSuccessMessage() throws InterruptedException {
        var callback = new ResponseMessageCallback();
        this.kafkaPublisherService.publishWithAsyncResponse(TOPIC, KEY, VALUE, callback);
        var actual = callback.getMessage();

        assertTrue(actual.startsWith("Message successfully sent to topic: %s".formatted(TOPIC)));
    }

}
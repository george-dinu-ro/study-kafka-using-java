package my.work.serialization.inbuilt.publisher;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InbuiltPublisherServiceTest {

    private static final String TOPIC = "test-publisher-topic-ds";

    private static final String KEY = "pc";

    private static final Integer VALUE = 5;

    private InbuiltPublisherService inbuiltPublisherService;

    @BeforeEach
    void beforeEach() {
        inbuiltPublisherService = new InbuiltPublisherService();
    }

    @Test
    void whenCallPublishWithoutResponse_thenShouldReturnStaticSuccessMessage() {
        var actual = this.inbuiltPublisherService.publishWithoutResponse(TOPIC, KEY, VALUE);

        assertEquals("Message successfully sent to topic: %s".formatted(TOPIC), actual);
    }

    @Test
    void whenCallPublishWithFutureResponse_thenShouldReturnDynamicSuccessMessage() {
        var actual = this.inbuiltPublisherService.publishWithFutureResponse(TOPIC, KEY, VALUE);

        assertTrue(actual.startsWith("Message successfully sent to topic: %s".formatted(TOPIC)));
    }

    @Test
    void whenCallSendMessageWithCallbackResponse_thenShouldReturnDynamicSuccessMessage() throws InterruptedException {
        var callback = new InbuiltResponseMessageCallback();
        this.inbuiltPublisherService.publishWithAsyncResponse(TOPIC, KEY, VALUE, callback);
        var actual = callback.getMessage();

        assertTrue(actual.startsWith("Message successfully sent to topic: %s".formatted(TOPIC)));
    }

}
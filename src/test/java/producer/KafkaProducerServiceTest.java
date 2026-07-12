package producer;

import message.KafkaProductMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaProducerServiceTest {

    private static final String TOPIC = "test-producer-topic";

    private static final String KEY = "pc";

    private static final Integer VALUE = 5;

    private KafkaProducerService kafkaProducerService;

    private KafkaProductMessage message;

    @BeforeEach
    void beforeEach() {
        kafkaProducerService = new KafkaProducerService();
        message = new KafkaProductMessage(TOPIC, KEY, VALUE);
    }

    @Test
    void whenCallSendMessageWithoutResponse_thenShouldReturnStaticSuccessMessage() {
        var actual = this.kafkaProducerService.sendMessageWithoutResponse(this.message);

        assertEquals(String.format("Message successfully sent to topic: %s", TOPIC), actual);
    }

    @Test
    void whenCallSendMessageWithFutureResponse_thenShouldReturnDynamicSuccessMessage() {
        var actual = this.kafkaProducerService.sendMessageWithFutureResponse(this.message);

        assertTrue(actual.startsWith(String.format("Message successfully sent to topic: %s", TOPIC)));
    }

    @Test
    void whenCallSendMessageWithCallbackResponse_thenShouldReturnDynamicSuccessMessage() throws InterruptedException {
        var callback = new ResponseMessageCallback();
        this.kafkaProducerService.sendMessageWithAsyncResponse(this.message, callback);
        var actual = callback.getMessage();

        assertTrue(actual.startsWith(String.format("Message successfully sent to topic: %s", TOPIC)));
    }

}
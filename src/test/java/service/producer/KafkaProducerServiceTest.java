package service.producer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaProducerServiceTest {

    private KafkaProducerService kafkaProducerService;

    @BeforeEach
    void beforeEach() {
        kafkaProducerService = new KafkaProducerService();
    }

    @Test
    void whenCallSendMessageWithoutResponse_thenShouldReturnStaticSuccessMessage() {
        var actual = kafkaProducerService.sendMessageWithoutResponse();

        assertEquals("Message successfully sent to topic: products-topic", actual);
    }

    @Test
    void whenCallSendMessageWithoutResponse_thenShouldReturnDynamicSuccessMessage() {
        var actual = kafkaProducerService.sendMessageWithFutureResponse();

        assertTrue(actual.startsWith("Message successfully sent to topic: products-topic"));
    }

}
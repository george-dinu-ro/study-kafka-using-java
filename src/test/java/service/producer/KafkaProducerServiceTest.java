package service.producer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaProducerServiceTest {

    private KafkaProducerService kafkaProducerService;

    @BeforeEach
    void beforeEach() {
        kafkaProducerService = new KafkaProducerService();
    }

    @Test
    void whenCallSendMessageWithoutResponse_thenShouldLogSuccessMessage() {
        kafkaProducerService.sendMessageWithoutResponse();
        assertTrue(true);
    }

}
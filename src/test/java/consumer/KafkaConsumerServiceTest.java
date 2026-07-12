package consumer;

import message.KafkaProductMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import producer.KafkaProducerService;

import static org.junit.jupiter.api.Assertions.assertEquals;

class KafkaConsumerServiceTest {

    private static final String TOPIC = "test-consumer-topic";

    private static final String KEY = "laptop";

    private static final Integer VALUE = 10;

    private KafkaProducerService kafkaProducerService;

    private KafkaConsumerService kafkaConsumerService;

    private KafkaProductMessage message;

    @BeforeEach
    void beforeEach() {
        this.kafkaProducerService = new KafkaProducerService();
        this.kafkaConsumerService = new KafkaConsumerService();
        message = new KafkaProductMessage(TOPIC, KEY, VALUE);
    }

    @Test
    void whenCallGetMessages_thenGetAllMessages() {
        this.kafkaProducerService.sendMessageWithoutResponse(this.message);
        var actual = this.kafkaConsumerService.getMessage(TOPIC);

        var currentMessage = actual.iterator().next();

        assertEquals(1, actual.count());

        assertEquals(TOPIC, currentMessage.topic());
    }

}
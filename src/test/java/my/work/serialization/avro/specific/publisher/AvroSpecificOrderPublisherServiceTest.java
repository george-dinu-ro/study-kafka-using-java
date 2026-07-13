package my.work.serialization.avro.specific.publisher;

import my.work.serialization.avro.specific.message.AvroOrder;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AvroSpecificOrderPublisherServiceTest {

    @Test
    void whenCallPublish_thenShouldReturnMetadata() {
        var topic = "test-publisher-topic-specific-avro";
        var order = new AvroOrder("John Doe", "IPhone", 1);

        var publisherService = new AvroSpecificOrderPublisherService();
        var metadata = publisherService.publish(topic, order.getCustomerName().toString(), order);

        assertEquals(topic, metadata.topic());
    }

}
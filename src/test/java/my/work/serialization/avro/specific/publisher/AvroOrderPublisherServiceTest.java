package my.work.serialization.avro.specific.publisher;

import my.work.serialization.avro.message.AvroOrder;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AvroOrderPublisherServiceTest {

    @Test
    void whenCallPublish_thenShouldReturnMetadata() {
        var topic = "test-publisher-topic-avro";
        var order = new AvroOrder("John Doe", "IPhone", 1);

        var publisherService = new AvroOrderPublisherService();
        var metadata = publisherService.publish(topic, order.getCustomerName().toString(), order);

        assertEquals(topic, metadata.topic());
    }

}
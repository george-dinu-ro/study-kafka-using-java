package my.work.serialization.avro.generic.publisher;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AvroGenericOrderPublisherServiceTest {

    @Test
    void whenCallPublish_thenShouldReturnMetadata() throws IOException {
        var topic = "test-publisher-topic-generic-avro";
        var customerName = "John Doe";
        var product = "IPhone";
        var quantity = 1;

        var publisherService = new AvroGenericOrderPublisherService();
        var metadata = publisherService.publish(topic, customerName, customerName, product, quantity);

        assertEquals(topic, metadata.topic());
    }

}
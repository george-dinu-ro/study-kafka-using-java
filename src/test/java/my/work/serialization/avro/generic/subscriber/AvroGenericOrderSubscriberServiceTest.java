package my.work.serialization.avro.generic.subscriber;

import my.work.serialization.avro.generic.publisher.AvroGenericOrderPublisherService;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AvroGenericOrderSubscriberServiceTest {

    @Test
    void whenCallSubscribe_thenGetMessage() throws IOException {
        var topic = "test-subscriber-topic-generic-avro";
        var customerName = "John Doe";
        var product = "IPhone";
        var quantity = 1;

        var publisherService = new AvroGenericOrderPublisherService();
        var subscriberService = new AvroGenericOrderSubscriberService();

        publisherService.publish(topic, customerName, customerName, product, quantity);

        var messages = subscriberService.subscribe(topic);
        var firstMessage = messages.iterator().next();

        assertEquals(topic, firstMessage.topic());

        assertEquals(customerName, firstMessage.key());

        assertEquals(customerName, firstMessage.value().get("customerName").toString());

        assertEquals(product, firstMessage.value().get("product").toString());

        assertEquals(quantity, Integer.parseInt(firstMessage.value().get("quantity").toString()));
    }

}
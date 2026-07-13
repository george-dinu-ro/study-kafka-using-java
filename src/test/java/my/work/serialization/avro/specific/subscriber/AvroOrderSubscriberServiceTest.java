package my.work.serialization.avro.specific.subscriber;

import my.work.serialization.avro.message.AvroOrder;
import my.work.serialization.avro.specific.publisher.AvroOrderPublisherService;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AvroOrderSubscriberServiceTest {

    @Test
    void whenCallSubscribe_thenGetMessage() {
        var topic = "test-subscriber-topic-avro";
        var order = new AvroOrder("John Doe", "IPhone", 1);

        var publisherService = new AvroOrderPublisherService();
        var subscriberService = new AvroOrderSubscriberService();

        publisherService.publish(topic, order.getCustomerName().toString(), order);

        var messages = subscriberService.subscribe(topic);
        var firstMessage = messages.iterator().next();

        assertEquals(topic, firstMessage.topic());

        assertEquals(order.getCustomerName().toString(), firstMessage.key());

        assertEquals(order, firstMessage.value());
    }

}
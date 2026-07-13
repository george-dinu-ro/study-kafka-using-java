package my.work.serialization.avro.specific.subscriber;

import my.work.serialization.avro.specific.message.AvroOrder;
import my.work.serialization.avro.specific.publisher.AvroSpecificOrderPublisherService;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AvroSpecificOrderSubscriberServiceTest {

    @Test
    void whenCallSubscribe_thenGetMessage() {
        var topic = "test-subscriber-topic-specific-avro";
        var order = new AvroOrder("John Doe", "IPhone", 1);

        var publisherService = new AvroSpecificOrderPublisherService();
        var subscriberService = new AvroSpecificOrderSubscriberService();

        publisherService.publish(topic, order.getCustomerName().toString(), order);

        var messages = subscriberService.subscribe(topic);
        var firstMessage = messages.iterator().next();

        assertEquals(topic, firstMessage.topic());

        assertEquals(order.getCustomerName().toString(), firstMessage.key());

        assertEquals(order, firstMessage.value());
    }

}
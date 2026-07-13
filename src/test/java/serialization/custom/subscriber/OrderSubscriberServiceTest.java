package serialization.custom.subscriber;

import org.junit.jupiter.api.Test;
import serialization.custom.message.Order;
import serialization.custom.publisher.OrderPublisherService;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class OrderSubscriberServiceTest {

    @Test
    void whenCallSubscribe_thenGetMessage() {
        var topic = "test-subscriber-topic-cs";
        var order = Order.builder().customerName("John Doe").product("IPhone").quantity(1).build();

        var publisherService = new OrderPublisherService();
        var subscriberService = new OrderSubscriberService();

        publisherService.publish(topic, order);

        var messages = subscriberService.subscribe(topic);
        var firstMessage = messages.iterator().next();

        assertEquals(topic, firstMessage.topic());

        assertNull(firstMessage.key());

        assertEquals(order, firstMessage.value());
    }

}
package my.work.serialization.custom.subscriber;

import org.junit.jupiter.api.Test;
import my.work.serialization.custom.message.CustomOrder;
import my.work.serialization.custom.publisher.OrderPublisherService;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class CustomOrderSubscriberServiceTest {

    @Test
    void whenCallSubscribe_thenGetMessage() {
        var topic = "test-subscriber-topic-cs";
        var order = CustomOrder.builder().customerName("John Doe").product("IPhone").quantity(1).build();

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
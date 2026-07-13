package my.work.serialization.custom.subscriber;

import org.junit.jupiter.api.Test;
import my.work.serialization.custom.message.CustomOrder;
import my.work.serialization.custom.publisher.CustomOrderPublisherService;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CustomCustomOrderSubscriberServiceTest {

    @Test
    void whenCallSubscribe_thenGetMessage() {
        var topic = "test-subscriber-topic-cs";
        var order = CustomOrder.builder().customerName("John Doe").product("IPhone").quantity(1).build();

        var publisherService = new CustomOrderPublisherService();
        var subscriberService = new CustomOrderSubscriberService();

        publisherService.publish(topic, order.customerName(), order);

        var messages = subscriberService.subscribe(topic);
        var firstMessage = messages.iterator().next();

        assertEquals(topic, firstMessage.topic());

        assertEquals(order.customerName(), firstMessage.key());

        assertEquals(order, firstMessage.value());
    }

}
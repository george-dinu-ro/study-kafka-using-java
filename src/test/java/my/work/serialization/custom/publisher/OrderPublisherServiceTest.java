package my.work.serialization.custom.publisher;

import my.work.serialization.custom.publisher.OrderPublisherService;
import org.junit.jupiter.api.Test;
import my.work.serialization.custom.message.Order;

import static org.junit.jupiter.api.Assertions.assertEquals;

class OrderPublisherServiceTest {

    @Test
    void whenCallPublish_thenShouldReturnMetadata() {
        var topic = "test-publisher-topic-cs";
        var order = Order.builder().customerName("John Doe").product("IPhone").quantity(1).build();

        var publisherService = new OrderPublisherService();
        var metadata = publisherService.publish(topic, order);

        assertEquals(topic, metadata.topic());
    }

}
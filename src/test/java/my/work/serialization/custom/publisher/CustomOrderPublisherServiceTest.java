package my.work.serialization.custom.publisher;

import org.junit.jupiter.api.Test;
import my.work.serialization.custom.message.CustomOrder;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CustomOrderPublisherServiceTest {

    @Test
    void whenCallPublish_thenShouldReturnMetadata() {
        var topic = "test-publisher-topic-cs";
        var order = CustomOrder.builder().customerName("John Doe").product("IPhone").quantity(1).build();

        var publisherService = new OrderPublisherService();
        var metadata = publisherService.publish(topic, order);

        assertEquals(topic, metadata.topic());
    }

}
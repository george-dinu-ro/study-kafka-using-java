package my.work.serialization.custom.publisher;

import org.junit.jupiter.api.Test;
import my.work.serialization.custom.message.CustomOrder;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CustomCustomOrderPublisherServiceTest {

    @Test
    void whenCallPublish_thenShouldReturnMetadata() {
        var topic = "test-publisher-topic-cs";
        var order = CustomOrder.builder().customerName("John Doe").product("IPhone").quantity(1).build();

        var publisherService = new CustomOrderPublisherService();
        var metadata = publisherService.publish(topic, order.customerName(), order);

        assertEquals(topic, metadata.topic());
    }

}
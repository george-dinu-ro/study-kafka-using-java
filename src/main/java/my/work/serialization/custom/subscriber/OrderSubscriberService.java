package my.work.serialization.custom.subscriber;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import my.work.serialization.custom.message.CustomOrder;
import my.work.serialization.custom.message.serializer.OrderDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OrderSubscriberService {

    public ConsumerRecords<String, CustomOrder> subscribe(String topic) {
        try (var consumer = getConsumer()) {
            consumer.subscribe(Collections.singletonList(topic));
            return consumer.poll(Duration.ofSeconds(5));
        }
    }

    private static KafkaConsumer<String, CustomOrder> getConsumer() {
        return new KafkaConsumer<>(getConfiguration());
    }

    private static Properties getConfiguration() {
        var props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.deserializer", StringDeserializer.class.getName());
        props.setProperty("value.deserializer", OrderDeserializer.class.getName());
        props.setProperty("auto.offset.reset", "earliest");
        props.setProperty("group.id", "product-group-cs");

        return props;
    }

}

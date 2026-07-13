package my.work.serialization.inbuilt.subscriber;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class InbuiltSubscriberService {

    public ConsumerRecords<String, Integer> subscribe(String topic) {
        try (var consumer = getConsumer()) {
            consumer.subscribe(Collections.singletonList(topic));
            return consumer.poll(Duration.ofSeconds(5));
        }
    }

    private static KafkaConsumer<String, Integer> getConsumer() {
        return new KafkaConsumer<>(getConfiguration());
    }

    private static Properties getConfiguration() {
        var props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.deserializer", StringDeserializer.class.getName());
        props.setProperty("value.deserializer", IntegerDeserializer.class.getName());
        props.setProperty("auto.offset.reset", "earliest");
        props.setProperty("group.id", "product-group-ds");

        return props;
    }

}

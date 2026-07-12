package consumer;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerService {

    private static Properties getConfiguration() {
        var props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.setProperty("group.id", "ProductGroup");

        return props;
    }

    private static KafkaConsumer<String, Integer> getConsumer() {
        return new KafkaConsumer<>(getConfiguration());
    }

    public ConsumerRecords<String, Integer> getMessage(String topic) {
        ConsumerRecords<String, Integer> response;

        try (var consumer = getConsumer()) {
            consumer.subscribe(Collections.singletonList(topic));
            consumer.seekToEnd(consumer.assignment());
            response = consumer.poll(Duration.ofSeconds(5));
        }

        return response;
    }

}

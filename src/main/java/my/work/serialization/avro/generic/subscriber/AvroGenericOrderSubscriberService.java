package my.work.serialization.avro.generic.subscriber;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class AvroGenericOrderSubscriberService {

    public ConsumerRecords<String, GenericRecord> subscribe(String topic) {
        try (var consumer = getConsumer()) {
            consumer.subscribe(Collections.singletonList(topic));
            return consumer.poll(Duration.ofSeconds(5));
        }
    }

    private KafkaConsumer<String, GenericRecord> getConsumer() {
        return new KafkaConsumer<>(getConfiguration());
    }

    private static Properties getConfiguration() {
        var props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:29092");
        props.setProperty("key.deserializer", KafkaAvroDeserializer.class.getName());
        props.setProperty("value.deserializer", KafkaAvroDeserializer.class.getName());
        props.setProperty("auto.offset.reset", "earliest");
        props.setProperty("group.id", "product-group-generic-avro");
        props.setProperty("schema.registry.url", "http://localhost:8081");

        return props;
    }

}

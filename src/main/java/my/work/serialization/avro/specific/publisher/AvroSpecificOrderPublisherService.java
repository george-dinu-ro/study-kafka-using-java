package my.work.serialization.avro.specific.publisher;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import my.work.serialization.avro.specific.message.AvroOrder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

public class AvroSpecificOrderPublisherService {

    public RecordMetadata publish(String topic, String key, AvroOrder order) {
        var kafkaMessage = getMessage(topic, key, order);

        try (var publisher = getPublisher()) {
            return publisher.send(kafkaMessage).get();

        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    private ProducerRecord<String, AvroOrder> getMessage(String topic, String key, AvroOrder order) {
        return new ProducerRecord<>(topic, key, order);
    }

    private Producer<String, AvroOrder> getPublisher() {
        return new KafkaProducer<>(getConfiguration());
    }

    private static Properties getConfiguration() {
        var props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:29092");
        props.setProperty("key.serializer", KafkaAvroSerializer.class.getName());
        props.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        props.setProperty("schema.registry.url", "http://localhost:8081");

        return props;
    }

}

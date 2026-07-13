package my.work.serialization.avro.generic.publisher;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.IOException;
import java.util.Properties;

public class AvroGenericOrderPublisherService {
    public RecordMetadata publish(String topic, String key, String customerName, String product, int quantity) throws IOException {
        var kafkaMessage = getMessage(topic, key, customerName, product, quantity);

        try (var publisher = getPublisher()) {
            return publisher.send(kafkaMessage).get();

        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    private ProducerRecord<String, GenericRecord> getMessage(String topic, String key, String customerName, String product, int quantity) throws IOException {
        var order = getOrder(customerName, product, quantity);
        return new ProducerRecord<>(topic, key, order);
    }

    private GenericData.Record getOrder(String customerName, String product, int quantity) throws IOException {
        var parser = new Schema.Parser();
        var schema = parser.parse(getClass().getResourceAsStream("/order.avsc"));

        var order = new GenericData.Record(schema);
        order.put("customerName", customerName);
        order.put("product", product);
        order.put("quantity", quantity);

        return order;
    }

    private Producer<String, GenericRecord> getPublisher() {
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

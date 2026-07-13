package my.work.serialization.custom.publisher;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import my.work.serialization.custom.message.Order;
import my.work.serialization.custom.message.serializer.OrderSerializer;

import java.util.Properties;

public class OrderPublisherService {

    public RecordMetadata publish(String topic, Order order) {
        var kafkaMessage = getMessage(topic, order);

        try (var publisher = getPublisher()) {
            return publisher.send(kafkaMessage).get();

        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    private static ProducerRecord<String, Order> getMessage(String topic, Order order) {
        return new ProducerRecord<>(topic, order);
    }

    private static Producer<String, Order> getPublisher() {
        return new KafkaProducer<>(getConfiguration());
    }

    private static Properties getConfiguration() {
        var props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.serializer", StringSerializer.class.getName());
        props.setProperty("value.serializer", OrderSerializer.class.getName());

        return props;
    }
}

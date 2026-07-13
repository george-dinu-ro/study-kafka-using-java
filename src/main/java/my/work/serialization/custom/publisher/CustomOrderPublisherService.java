package my.work.serialization.custom.publisher;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import my.work.serialization.custom.message.CustomOrder;
import my.work.serialization.custom.message.serializer.CustomOrderSerializer;

import java.util.Properties;

public class CustomOrderPublisherService {

    public RecordMetadata publish(String topic, String key, CustomOrder customOrder) {
        var kafkaMessage = getMessage(topic, key, customOrder);

        try (var publisher = getPublisher()) {
            return publisher.send(kafkaMessage).get();

        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    private static ProducerRecord<String, CustomOrder> getMessage(String topic, String key, CustomOrder customOrder) {
        return new ProducerRecord<>(topic, key, customOrder);
    }

    private static Producer<String, CustomOrder> getPublisher() {
        return new KafkaProducer<>(getConfiguration());
    }

    private static Properties getConfiguration() {
        var props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:29092");
        props.setProperty("key.serializer", StringSerializer.class.getName());
        props.setProperty("value.serializer", CustomOrderSerializer.class.getName());

        return props;
    }

}

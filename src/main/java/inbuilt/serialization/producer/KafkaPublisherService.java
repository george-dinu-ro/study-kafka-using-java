package inbuilt.serialization.producer;

import lombok.extern.slf4j.Slf4j;
import inbuilt.serialization.message.KafkaProductMessage;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

@Slf4j
public class KafkaPublisherService {

    public String publishWithoutResponse(KafkaProductMessage productMessage) {
        var message = getRecord(productMessage);

        try (var producer = getProducer()) {
            producer.send(message);
            return "Message successfully sent to topic: %s".formatted(message.topic());

        } catch (Exception e) {
            log.error("Error while sending message to topic: {}", message.topic(), e);
            return "Error while sending message to topic: %s".formatted(message.topic());
        }
    }

    public String publishWithFutureResponse(KafkaProductMessage productMessage) {
        var message = getRecord(productMessage);

        try (var producer = getProducer()) {
            var response = producer.send(message).get();
            return "Message successfully sent to topic: %s, partition: %s, offset: %s".formatted(response.topic(), response.partition(), response.offset());

        } catch (Exception e) {
            Thread.currentThread().interrupt();
            log.error("Error while sending message to topic: {}", message.topic(), e);
            return "Error while sending message to topic: %s".formatted(message.topic());
        }
    }

    public void publishWithAsyncResponse(KafkaProductMessage productMessage, Callback callback) {
        var message = getRecord(productMessage);

        try (var producer = getProducer()) {
            producer.send(message, callback);

        } catch (Exception e) {
            log.error("Error while sending message to topic: {}", message.topic(), e);
        }
    }

    private static ProducerRecord<String, Integer> getRecord(KafkaProductMessage productMessage) {
        return new ProducerRecord<>(productMessage.topic(), productMessage.key(), productMessage.value());
    }

    private static Producer<String, Integer> getProducer() {
        return new KafkaProducer<>(getConfiguration());
    }

    private static Properties getConfiguration() {
        var props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.serializer", StringSerializer.class.getName());
        props.setProperty("value.serializer", IntegerSerializer.class.getName());

        return props;
    }

}

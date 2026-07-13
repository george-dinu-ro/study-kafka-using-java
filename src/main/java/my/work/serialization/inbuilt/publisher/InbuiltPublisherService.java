package my.work.serialization.inbuilt.publisher;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

@Slf4j
public class InbuiltPublisherService {

    public String publishWithoutResponse(String topic, String key, Integer value) {
        var message = getRecord(topic, key, value);

        try (var producer = getProducer()) {
            producer.send(message);
            return "Message successfully sent to topic: %s".formatted(message.topic());

        } catch (Exception e) {
            log.error("Error while sending message to topic: {}", message.topic(), e);
            return "Error while sending message to topic: %s".formatted(message.topic());
        }
    }

    public String publishWithFutureResponse(String topic, String key, Integer value) {
        var message = getRecord(topic, key, value);

        try (var producer = getProducer()) {
            var response = producer.send(message).get();
            return "Message successfully sent to topic: %s, partition: %s, offset: %s".formatted(response.topic(), response.partition(), response.offset());

        } catch (Exception e) {
            log.error("Error while sending message to topic: {}", message.topic(), e);
            return "Error while sending message to topic: %s".formatted(message.topic());
        }
    }

    public void publishWithAsyncResponse(String topic, String key, Integer value, Callback callback) {
        var message = getRecord(topic, key, value);

        try (var producer = getProducer()) {
            producer.send(message, callback);

        } catch (Exception e) {
            log.error("Error while sending message to topic: {}", message.topic(), e);
        }
    }

    private static ProducerRecord<String, Integer> getRecord(String topic, String key, Integer value) {
        return new ProducerRecord<>(topic, key, value);
    }

    private static Producer<String, Integer> getProducer() {
        return new KafkaProducer<>(getConfiguration());
    }

    private static Properties getConfiguration() {
        var props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:29092");
        props.setProperty("key.serializer", StringSerializer.class.getName());
        props.setProperty("value.serializer", IntegerSerializer.class.getName());

        return props;
    }

}

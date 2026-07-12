package producer;

import lombok.extern.slf4j.Slf4j;
import message.KafkaProductMessage;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

@Slf4j
public class KafkaProducerService {

    private static Properties getConfiguration() {
        var props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("value.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");

        return props;
    }

    private static Producer<String, Integer> getProducer() {
        return new KafkaProducer<>(getConfiguration());
    }

    private static ProducerRecord<String, Integer> getRecord(KafkaProductMessage productMessage) {
        return new ProducerRecord<>(productMessage.topic(), productMessage.key(), productMessage.value());
    }

    public String sendMessageWithoutResponse(KafkaProductMessage productMessage) {
        var message = getRecord(productMessage);

        try (var producer = getProducer()) {
            producer.send(message);
            return String.format("Message successfully sent to topic: %s", message.topic());

        } catch (Exception e) {
            log.error("Error while sending message to topic: {}", message.topic(), e);
            return String.format("Error while sending message to topic: %s", message.topic());
        }
    }

    public String sendMessageWithFutureResponse(KafkaProductMessage productMessage) {
        var message = getRecord(productMessage);

        try (var producer = getProducer()) {
            var response = producer.send(message).get();
            return String.format("Message successfully sent to topic: %s, partition: %s, offset: %s", response.topic(), response.partition(), response.offset());

        } catch (Exception e) {
            log.error("Error while sending message to topic: {}", message.topic(), e);
            return String.format("Error while sending message to topic: %s", message.topic());
        }
    }

    public void sendMessageWithAsyncResponse(KafkaProductMessage productMessage, Callback callback) {
        var message = getRecord(productMessage);

        try (var producer = getProducer()) {
            producer.send(message, callback);

        } catch (Exception e) {
            log.error("Error while sending message to topic: {}", message.topic(), e);
        }
    }

}

package service.producer;

import lombok.extern.slf4j.Slf4j;
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

    private static ProducerRecord<String, Integer> getRecord() {
        return new ProducerRecord<>("products-topic", "laptop", 10);
    }

    public void sendMessageWithoutResponse() {
        var data = getRecord();

        try (var producer = getProducer()) {
            producer.send(data);
            log.info("Message successfully sent to topic: {}", data.topic());

        } catch (Exception e) {
            log.error("Error while sending record to topic: {}", data.topic(), e);
        }
    }

}

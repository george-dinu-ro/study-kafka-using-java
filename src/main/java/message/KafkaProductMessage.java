package message;

public record KafkaProductMessage(String topic, String key, Integer value) {

}

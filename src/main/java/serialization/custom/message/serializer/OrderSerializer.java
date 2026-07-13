package serialization.custom.message.serializer;

import org.apache.kafka.common.serialization.Serializer;
import serialization.custom.message.Order;

public class OrderSerializer implements Serializer<Order> {

    @Override
    public byte[] serialize(String topic, Order data) {
        return SerializerUtility.serialize(data);
    }
}

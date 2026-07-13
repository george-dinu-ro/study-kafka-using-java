package serialization.custom.message.serializer;

import org.apache.kafka.common.serialization.Deserializer;
import serialization.custom.message.Order;

public class OrderDeserializer implements Deserializer<Order> {

    @Override
    public Order deserialize(String topic, byte[] data) {
        return SerializerUtility.deserialize(data, Order.class);
    }

}

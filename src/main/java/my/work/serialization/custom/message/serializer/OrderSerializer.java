package my.work.serialization.custom.message.serializer;

import org.apache.kafka.common.serialization.Serializer;
import my.work.serialization.custom.message.CustomOrder;

public class OrderSerializer implements Serializer<CustomOrder> {

    @Override
    public byte[] serialize(String topic, CustomOrder data) {
        return SerializerUtility.serialize(data);
    }
}

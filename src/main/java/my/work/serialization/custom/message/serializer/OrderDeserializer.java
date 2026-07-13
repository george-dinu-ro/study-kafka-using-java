package my.work.serialization.custom.message.serializer;

import org.apache.kafka.common.serialization.Deserializer;
import my.work.serialization.custom.message.CustomOrder;

public class OrderDeserializer implements Deserializer<CustomOrder> {

    @Override
    public CustomOrder deserialize(String topic, byte[] data) {
        return SerializerUtility.deserialize(data, CustomOrder.class);
    }

}

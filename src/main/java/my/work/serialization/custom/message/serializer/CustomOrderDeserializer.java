package my.work.serialization.custom.message.serializer;

import org.apache.kafka.common.serialization.Deserializer;
import my.work.serialization.custom.message.CustomOrder;

public class CustomOrderDeserializer implements Deserializer<CustomOrder> {

    @Override
    public CustomOrder deserialize(String topic, byte[] data) {
        return CustomSerializerUtility.deserialize(data, CustomOrder.class);
    }

}

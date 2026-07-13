package serialization.custom.message.serializer;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import tools.jackson.databind.ObjectMapper;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class SerializerUtility {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static <T> byte[] serialize(T obj) {
        return MAPPER.writeValueAsBytes(obj);
    }

    public static <T> T deserialize(byte[] bytes, Class<T> clazz) {
        return MAPPER.readValue(bytes, clazz);
    }

}

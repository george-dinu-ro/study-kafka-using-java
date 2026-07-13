package serialization.custom.message.serializer;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import tools.jackson.databind.ObjectMapper;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class SerializerUtility {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static <T> byte[] serialize(T obj) {
        try {
            return MAPPER.writeValueAsBytes(obj);

        } catch (Exception e) {
            e.printStackTrace();
        }

        return new byte[0];
    }

    public static <T> T deserialize(byte[] bytes, Class<T> clazz) {
        try {
            return MAPPER.readValue(bytes, clazz);

        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

}

package producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Objects;
import java.util.concurrent.CountDownLatch;

@Slf4j
public class ResponseMessageCallback implements Callback {

    private final CountDownLatch countDownLatch = new CountDownLatch(1);

    private String message;

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (Objects.nonNull(exception)) {
            log.error("Error while sending message to topic: {}", metadata.topic());
            this.message = String.format("Error while sending message to topic: %s", metadata.topic());

        } else {
            log.info("Message successfully sent to topic: {}, partition: {}, offset: {}", metadata.topic(), metadata.partition(), metadata.offset());
            this.message = String.format("Message successfully sent to topic: %s, partition: %s, offset: %s", metadata.topic(), metadata.partition(), metadata.offset());
        }

        this.countDownLatch.countDown();
    }

    public String getMessage() throws InterruptedException {
        this.countDownLatch.await();
        return this.message;
    }

}

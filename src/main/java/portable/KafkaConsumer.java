package portable;

import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOffset;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;

import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

/**
 * Blog post:
 * http://kojotdev.com/2019/08/spring-webflux-websocket-with-vue-js/
 * <p>
 * <p>
 * SO Post:
 * https://stackoverflow.com/questions/54248336/springboot2-webflux-websocket
 * <p>
 * And this tutorial by Josh Long:
 * https://www.youtube.com/watch?v=GlvyHIqT3K4
 */
@Log4j2
@Service
public class KafkaConsumer implements Consumer<FluxSink<String>> {

    // This is queue of messages that will be pushed to websocket client
    // This does retain the history of all messages, just the new messages that need to be pushed
    private final BlockingQueue<String> queue = new LinkedBlockingQueue<>();
    private final Executor executor = Executors.newSingleThreadExecutor();

    static String BOOTSTRAP_SERVERS = System.getenv("BOOTSTRAP_SERVER");
    static final String TOPIC = System.getenv("KAFKA_TOPIC");

    final ApplicationEventPublisher publisher;

    public KafkaConsumer(ApplicationEventPublisher publisher) {
        this.publisher = publisher;
    }

    public ReceiverOptions<Integer, String> receiverOptions() {

        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "sample-consumer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "sample-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return ReceiverOptions.create(props);
    }

    Disposable consumeMessages() {

        log.info(" starting SampleConsumer.consumeMessages");
        SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss:SSS z dd MMM yyyy");

        ReceiverOptions<Integer, String> options = receiverOptions().subscription(Collections.singleton(TOPIC))
                .addAssignListener(partitions -> log.debug("onPartitionsAssigned {}", partitions))
                .addRevokeListener(partitions -> log.debug("onPartitionsRevoked {}", partitions));
        Flux<ReceiverRecord<Integer, String>> kafkaFlux = KafkaReceiver.create(options).receive();

        return kafkaFlux.subscribe(record -> {
            //Put the record value from kafka into the WS queue for sending
            push(record.value());
            ReceiverOffset offset = record.receiverOffset();
            String text = String.format("Received message: topic-partition=%s offset=%d timestamp=%s key=%s value=%s\n",
                    offset.topicPartition(),
                    offset.offset(),
                    dateFormat.format(new Date(record.timestamp())),
                    record.key(),
                    record.value().length());
            log.debug(text);
            offset.acknowledge();
        });
    }

    //Push a message onto the queue for sending
    public boolean push(String message) {
        return queue.offer(message);
    }

    //This sets up the flux sink which is plugged into the WS session
    //It continuously pulls messages from the queue and when it finds one it sends it to the WS
    //Note that this is a blocking queue so it is run a separate executor thread
    @Override
    public void accept(FluxSink<String> sink) {
        this.executor.execute(() -> {
            while (true) {
                try {
                    final String message = queue.take();
                    sink.next(message);
                } catch (InterruptedException exc) {
                    log.error("Could not take greeting from queue", exc);
                }
            }
        });
    }
}

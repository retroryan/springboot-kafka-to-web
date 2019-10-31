package com.example.socketclient;

import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOffset;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;

import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@Log4j2
@Service
public class KafkaConsumer {


    static final String BOOTSTRAP_SERVERS = "localhost:9092";

    static final String TOPIC = "stackoverflow-questions";

    final ApplicationEventPublisher publisher;

    public KafkaConsumer(ApplicationEventPublisher publisher) {
        this.publisher = publisher;
    }

    private ReceiverOptions<Integer, String> receiverOptions() {

        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "sample-consumer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "sample-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
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
            ReceiverOffset offset = record.receiverOffset();

            this.publisher.publishEvent(new KafkaMessageEvent(record));

            /*System.out.printf("Received message: topic-partition=%s offset=%d timestamp=%s key=%d value=%s\n",
                    offset.topicPartition(),
                    offset.offset(),
                    dateFormat.format(new Date(record.timestamp())),
                    record.key(),
                    record.value().length());*/

            offset.acknowledge();
        });
    }

}

package com.example.socketclient;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.reactive.HandlerMapping;
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.server.support.WebSocketHandlerAdapter;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOffset;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class AltWebSocketServerApp {

    static final String BOOTSTRAP_SERVERS = "localhost:9092";
    static final String TOPIC = "stackoverflow-questions";

    ReceiverOptions<Integer, String> receiverOptions() {

        System.out.println("SampleConsumer.SampleConsumer");

        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "sample-consumer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "sample-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        ReceiverOptions<Integer, String> receiverOptions = ReceiverOptions.create(props);
        return receiverOptions;
    }

    WebSocketHandlerAdapter webSocketHandlerAdapter() {
        return new WebSocketHandlerAdapter();
    }


    WebSocketHandler webSocketHandler() {

        //  TODO - Is there a way to plug this kafka flux directly into the WS?
        ReceiverOptions<Integer, String> options = receiverOptions().subscription(Collections.singleton(TOPIC))
                .addAssignListener(partitions -> log.debug("onPartitionsAssigned {}", partitions))
                .addRevokeListener(partitions -> log.debug("onPartitionsRevoked {}", partitions));
        SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss:SSS z dd MMM yyyy");


        Flux<ReceiverRecord<Integer, String>> kafkaFlux = KafkaReceiver.create(options).receive();
        kafkaFlux.subscribe(record -> {
            ReceiverOffset offset = record.receiverOffset();
            System.out.printf("Received message: topic-partition=%s offset=%d timestamp=%s key=%d value=%s\n",
                    offset.topicPartition(),
                    offset.offset(),
                    dateFormat.format(new Date(record.timestamp())),
                    record.key(),
                    record.value());
            offset.acknowledge();
        });

        return session -> {
            return session.send(
                    Flux.interval(Duration.ofSeconds(1))
                            .map(n -> {
                                String nextMessage = "{'cnt':,'" + n.toString() + "'}";
                                log.info("Sending msg: " + nextMessage);
                                return nextMessage;
                            })
                            .map(session::textMessage)
            );
        };
    }

    HandlerMapping webSocketURLMapping() {

        SimpleUrlHandlerMapping simpleUrlHandlerMapping = new SimpleUrlHandlerMapping();
        simpleUrlHandlerMapping.setUrlMap(
                Collections.singletonMap("/ws/feed", webSocketHandler()));
        simpleUrlHandlerMapping.setCorsConfigurations(
                Collections.singletonMap("*", new CorsConfiguration().applyPermitDefaultValues()));
        simpleUrlHandlerMapping.setOrder(10);
        return simpleUrlHandlerMapping;
    }
}
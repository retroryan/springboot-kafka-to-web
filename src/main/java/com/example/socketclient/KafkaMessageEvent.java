package com.example.socketclient;

import org.springframework.context.ApplicationEvent;
import reactor.kafka.receiver.ReceiverRecord;

class KafkaMessageEvent extends ApplicationEvent {

    KafkaMessageEvent(ReceiverRecord<Integer, String> record) {
        super(record);
    }
}

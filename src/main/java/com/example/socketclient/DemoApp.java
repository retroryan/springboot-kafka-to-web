package com.example.socketclient;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import reactor.core.Disposable;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
@Slf4j
public class DemoApp {

    @Bean
    ApplicationRunner appRunner(KafkaConsumer kafkaConsumer) {
        return args -> {
            final CountDownLatch latch = new CountDownLatch(1);

            log.info("Starting consumer " + kafkaConsumer);
            Disposable disposable = kafkaConsumer.consumeMessages();

            latch.await(600, TimeUnit.SECONDS);

            //disposable.dispose();
        };
    }

    public static void main(String[] args) throws Exception {
        SpringApplication.run(DemoApp.class);
    }


}

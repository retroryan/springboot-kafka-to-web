package com.example.socketclient;

import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;
import org.springframework.util.ReflectionUtils;
import reactor.core.publisher.FluxSink;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

/**
 * Adopted from this blog post:
 * https://developer.okta.com/blog/2018/09/24/reactive-apis-with-spring-webflux
 */
@Component
class KafkaMessageEventPublisher implements
        ApplicationListener<KafkaMessageEvent>,
        Consumer<FluxSink<KafkaMessageEvent>> {

    private final Executor executor;
    private final BlockingQueue<KafkaMessageEvent> queue =
            new LinkedBlockingQueue<>();

    KafkaMessageEventPublisher(Executor executor) {

        this.executor = executor;

        System.out.println("executor = " + executor);
    }

    @Override
    public void onApplicationEvent(KafkaMessageEvent event) {
        this.queue.offer(event);
    }

    @Override
    public void accept(FluxSink<KafkaMessageEvent> sink) {
        this.executor.execute(() -> {
            while (true)
                try {
                    KafkaMessageEvent event = queue.take();
                    sink.next(event);
                } catch (InterruptedException e) {
                    ReflectionUtils.rethrowRuntimeException(e);
                }
        });
    }
}

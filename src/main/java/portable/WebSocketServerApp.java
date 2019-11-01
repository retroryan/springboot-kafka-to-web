package portable;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.HandlerMapping;
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.server.support.WebSocketHandlerAdapter;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.ReceiverRecord;

import java.util.Collections;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

@Slf4j
@Configuration
public class WebSocketServerApp {

    @Bean
    Executor executor() {
        return Executors.newSingleThreadExecutor();
    }

    @Bean
    WebSocketHandlerAdapter webSocketHandlerAdapter() {
        return new WebSocketHandlerAdapter();
    }

    @Bean
    WebSocketHandler webSocketHandler(KafkaMessageEventPublisher eventPublisher) {
        Flux<KafkaMessageEvent> publish = Flux
                .create(eventPublisher)
                .share();

        log.info("Connected new WS client");

        return session -> {
            Flux<WebSocketMessage> messageFlux = publish
                    .map(evt -> {
                        ReceiverRecord<Integer, String> record = (ReceiverRecord<Integer, String>) evt.getSource();
                        return record.value();
                    })
                    .map(session::textMessage);
            return session.send(messageFlux);
        };
    }

    @Bean
    HandlerMapping handlerMapping(WebSocketHandler wsh) {
        return new SimpleUrlHandlerMapping() {
            {
                setUrlMap(Collections.singletonMap("/questions", wsh));
                setOrder(10);
            }
        };
    }
}
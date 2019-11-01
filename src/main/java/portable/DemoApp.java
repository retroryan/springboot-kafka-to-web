package portable;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.Resource;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.Disposable;

import static org.springframework.web.reactive.function.server.RequestPredicates.GET;
import static org.springframework.web.reactive.function.server.RouterFunctions.route;
import static org.springframework.web.reactive.function.server.ServerResponse.ok;

@SpringBootApplication
@Slf4j
public class DemoApp {

    //This loads the index.html when you hit / to force a default homepage
    @Bean
    public RouterFunction<ServerResponse> htmlRouter(
            @Value("classpath:/static/index.html") Resource html) {
        return route(GET("/"), request
                -> ok().contentType(MediaType.TEXT_HTML).syncBody(html)
        );
    }

    @Bean
    ApplicationRunner appRunner(KafkaConsumer kafkaConsumer) {
        return args -> {
            log.info("Starting consumer " + kafkaConsumer);
            Disposable disposable = kafkaConsumer.consumeMessages();
        };
    }

    public static void main(String[] args) throws Exception {
        SpringApplication.run(DemoApp.class);
    }


}

package portable;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import reactor.core.Disposable;

@SpringBootApplication
@Slf4j
public class DemoApp {

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

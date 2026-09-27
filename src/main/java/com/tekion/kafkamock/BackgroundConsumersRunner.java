package com.tekion.kafkamock;

import com.tekion.kafkamock.model.Message;
import com.tekion.kafkamock.service.BrokerService;
import com.tekion.kafkamock.service.ConsumerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
@Slf4j
@RequiredArgsConstructor
public class BackgroundConsumersRunner implements CommandLineRunner {

    private final BrokerService brokerService;
    private final ConsumerService consumerService;

    private ExecutorService executor;

    @Override
    public void run(String... args) {
        String topic = "orders";
        int partitions = 4;
        String groupId = "g1";
        int consumerThreads = 2;

   
        brokerService.createTopic(topic, partitions);

        executor = Executors.newFixedThreadPool(consumerThreads);

        for (int i = 1; i <= consumerThreads; i++) {
            String consumerName = "c" + i;
            executor.submit(() -> pollLoop(topic, groupId, consumerName));
        }

        log.info("System Ready: Topic '{}' initialized. Background consumers c1, c2 are polling...", topic);
    }

    private void pollLoop(String topic, String groupId, String consumerName) {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                // This keeps running. When you hit the API, this will catch the new messages.
                List<Message> messages = consumerService.poll(topic, groupId, consumerName);

                for (Message m : messages) {
                    log.info("[group={} consumer={}] RECEIVED: key={} offset={} value={}",
                            groupId, consumerName, m.getKey(), m.getOffset(), m.getValue());
                }
                Thread.sleep(300); 
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                log.error("[group={} consumer={}] poll error", groupId,consumerName, e);
            }
        }
    }

    @jakarta.annotation.PreDestroy
    public void shutdown() {
        if (executor != null) executor.shutdownNow();
    }
}
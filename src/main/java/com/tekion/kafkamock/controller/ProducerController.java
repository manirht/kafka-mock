package com.tekion.kafkamock.controller;

import com.tekion.kafkamock.service.BrokerService;
import lombok.RequiredArgsConstructor; // Lombok: Generates constructor for @Autowired
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/producer")
@RequiredArgsConstructor
public class ProducerController {
    private final BrokerService brokerService;

    @PostMapping("/setup")
    public String setup(@RequestParam String topic, @RequestParam int partitions) {
        brokerService.createTopic(topic, partitions);
        return "Topic initialized.";
    }

    @PostMapping("/send")
    public String send(@RequestParam String topic, @RequestParam String key, @RequestParam String value) {
        brokerService.produce(topic, key, value);
        return "Sent.";
    }
}
// package com.tekion.kafkamock.controller;

// import com.tekion.kafkamock.model.Message;
// import com.tekion.kafkamock.service.ConsumerService;
// import lombok.RequiredArgsConstructor;
// import org.springframework.web.bind.annotation.*;

// import java.util.List;

// // @RestController
// // @RequestMapping("/api/consumer")
// // @RequiredArgsConstructor
// public class ConsumerController {

//     private final ConsumerService consumerService;

//     @GetMapping("/poll")
//     public List<Message> poll(
//             @RequestParam String topic,
//             @RequestParam String groupId,
//             @RequestParam String consumerName) {
//         return consumerService.poll(topic, groupId, consumerName);
//     }

//     // Demonstrates Replay (Task 5)
//     @PostMapping("/reset")
//     public String reset(
//             @RequestParam String topic,
//             @RequestParam String groupId,
//             @RequestParam String consumerName) {
//         consumerService.resetOffsets(topic, groupId, consumerName);
//         return "Offsets reset to 0 for " + consumerName + " in group " + groupId;
//     }
// }
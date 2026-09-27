package com.tekion.kafkamock.service;

import com.tekion.kafkamock.model.Message;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Service
@Slf4j
@RequiredArgsConstructor
public class ConsumerService {

    private final BrokerService brokerService;
    private final GroupCoordinator groupCoordinator;

    // Stores offsets as Map<"groupId:topic:partitionId", offsetValue>
    private final Map<String, Integer> offsetStore = new ConcurrentHashMap<>();

    public List<Message> poll(String topic, String groupId, String consumerName) {
        // 1. Register with coordinator to handle Task 4 (Groups/Partitions)
        groupCoordinator.joinGroup(groupId, consumerName);
        
        int totalPartitions = brokerService.getPartitionCount(topic);
        List<Integer> assignedPartitions = groupCoordinator.getAssignedPartitions(groupId, consumerName, totalPartitions);
        
        List<Message> allMessages = new ArrayList<>();

        // 2. Read from each assigned partition
        for (int partitionId : assignedPartitions) {
            String stateKey = String.format("%s:%s:%d", groupId, topic, partitionId);
            int currentOffset = offsetStore.getOrDefault(stateKey, 0);

            List<Message> messages = brokerService.fetch(topic, partitionId, currentOffset);
            
            if (!messages.isEmpty()) {
                allMessages.addAll(messages);
                // Update offset to the next available position
                int lastOffset = messages.get(messages.size() - 1).getOffset();
                offsetStore.put(stateKey, lastOffset + 1);
            }
        }
        
        return allMessages;
    }

    // Task 5: Replay functionality
    public void resetOffsets(String topic, String groupId, String consumerName) {
        int totalPartitions = brokerService.getPartitionCount(topic);
        List<Integer> assignedPartitions = groupCoordinator.getAssignedPartitions(groupId, consumerName, totalPartitions);
        
        for (int partitionId : assignedPartitions) {
            String stateKey = String.format("%s:%s:%d", groupId, topic, partitionId);
            offsetStore.put(stateKey, 0); // Reset to beginning of the log
            log.info("Reset offset for Group: {}, Partition: {}", groupId, partitionId);
        }
    }
}
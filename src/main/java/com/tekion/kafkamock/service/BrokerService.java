package com.tekion.kafkamock.service;

import com.tekion.kafkamock.model.Message;
import com.tekion.kafkamock.repository.TopicPartition;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Service
@Slf4j // Lombok annotation for easy logging
public class BrokerService {
    private final Map<String, List<TopicPartition>> topics = new ConcurrentHashMap<>();

    public void createTopic(String name, int partitions) {
        List<TopicPartition> pList = new ArrayList<>();
        for (int i = 0; i < partitions; i++) pList.add(new TopicPartition());
        topics.put(name, pList);
        log.info("Topic {} created with {} partitions", name, partitions);
    }

    public void produce(String topic, String key, String value) {
        List<TopicPartition> partitions = topics.get(topic);
        if (partitions == null) throw new RuntimeException("Topic not found");

        int partitionId = Math.abs(key.hashCode() % partitions.size());
        int offset = partitions.get(partitionId).append(key, value);
        log.info("Produced to {}-P{} at offset {}", topic, partitionId, offset);
    }

    public List<Message> fetch(String topic, int partitionId, int offset) {
        return topics.get(topic).get(partitionId).readFrom(offset);
    }
    
    public int getPartitionCount(String topic) {
        return topics.containsKey(topic) ? topics.get(topic).size() : 0;
    }
}
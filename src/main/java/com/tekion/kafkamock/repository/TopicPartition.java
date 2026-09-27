package com.tekion.kafkamock.repository;

import com.tekion.kafkamock.model.Message;
import org.springframework.stereotype.Repository;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

@Repository
public class TopicPartition {
    private final List<Message> log = new CopyOnWriteArrayList<>();

    public synchronized int append(String key, String value) {
        int offset = log.size();
        Message msg = Message.builder()
                .key(key)
                .value(value)
                .timestamp(System.currentTimeMillis())
                .offset(offset)
                .build();
        log.add(msg);
        return offset;
    }

    public List<Message> readFrom(int offset) {
        if (offset >= log.size()) return Collections.emptyList();
        return new ArrayList<>(log.subList(offset, log.size()));
    }
    
    // Task 5: Retention helper
    public void trim(int maxMessages) {
        while (log.size() > maxMessages) {
            log.remove(0);
        }
    }
}
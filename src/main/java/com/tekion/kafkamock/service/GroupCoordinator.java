package com.tekion.kafkamock.service;

import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class GroupCoordinator {
    // Map of GroupID -> Set of Consumer Names
    private final Map<String, Set<String>> groupMembers = new ConcurrentHashMap<>();

    public void joinGroup(String groupId, String consumerName) {
        groupMembers.computeIfAbsent(groupId, k -> ConcurrentHashMap.newKeySet()).add(consumerName);
    }

    public List<Integer> getAssignedPartitions(String groupId, String consumerName, int totalPartitions) {
        Set<String> memberSet = groupMembers.getOrDefault(groupId, Set.of());
        if (memberSet.isEmpty() || totalPartitions <= 0) return List.of();

        List<String> members = new ArrayList<>(memberSet);
        Collections.sort(members);

        int memberIndex = members.indexOf(consumerName);
        if (memberIndex < 0) return List.of();

        List<Integer> assigned = new ArrayList<>();

        // Round-robin assignment
        for (int i = 0; i < totalPartitions; i++) {
            if (i % members.size() == memberIndex) assigned.add(i);
        }
        return assigned;
    }
}
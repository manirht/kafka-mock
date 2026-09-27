package com.tekion.kafkamock.model;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Message {
    private String key;
    private String value;
    private long timestamp;
    private int offset;
}
package org.example;

public class KafkaEvent {
    public String topic;
    public String value;
    public String key;
    public long partition;
    public long offset;
    public long timestamp;


    public KafkaEvent(String key, String value, String topic, long metadataPartition, long metadataOffset,
                      long metadataTimestamp){
        this.topic = topic;
        this.key = key;
        this.value = value;
        this.partition = metadataPartition;
        this.offset = metadataOffset;
        this.timestamp = metadataTimestamp;

    }

}

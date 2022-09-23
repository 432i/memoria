package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class StreamsTimestampExtractor {
    static class myTimestampExtractor implements TimestampExtractor {
        @Override
        public long extract(ConsumerRecord<Object, Object> record, long partitionTime){
            System.out.println( "su timestamp es: "+ record.timestamp() );
            return record.timestamp();
        }

    }
}

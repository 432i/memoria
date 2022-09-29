package org.example;

import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.DoFn;

public class StreamOperation extends DoFn<KafkaRecord<String, String>, Integer> {
    @ProcessElement
    public void processElement(@Element KafkaRecord<String, String> word, OutputReceiver<Integer> out) {
        // Use OutputReceiver.output to emit the output element.
        System.out.println("timestamp:"+word.getTimestamp()+
                ", llave:"+word.getKV().getKey()+", valor:"+word.getKV().getValue());
        out.output(1);
    }
}
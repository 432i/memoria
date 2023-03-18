package org.example;

import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.DoFn;

public class StreamOperation extends DoFn<KafkaRecord<String, String>, String> {
    @ProcessElement
    public void processElement(@Element KafkaRecord<String, String> word, OutputReceiver<String> out) {
        // Use OutputReceiver.output to emit the output element.
        out.output("");
    }
}

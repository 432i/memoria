package org.example;


import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class App
{
    public static void main( String[] args ) {
        System.out.println( "conecting to kafka from apache-beam" );
        Pipeline pipeline = Pipeline.create();
        PCollection<KafkaRecord<Long, String>> pCollection = pipeline.apply(KafkaIO.<Long, String>read()
                        .withBootstrapServers("34.176.50.2:9092")
                        .withTopic("test3")
                        .withKeyDeserializer(LongDeserializer.class)
                        .withValueDeserializer(StringDeserializer.class)
        );
        pCollection.apply(ParDo.of(new DoFn<String, String>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                System.out.println(String.format("** element |%s| **",
                        c.element()));
            }
        }));
        //Here we are starting the pipeline
        pipeline.run();

    }
}

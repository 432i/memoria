package org.example;


import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.StringDeserializer;

public class App
{
    public static void main( String[] args ) {
        System.out.println( "conecting to kafka from apache-beam" );
        Pipeline pipeline = Pipeline.create();
        PCollection<KafkaRecord<String, String>> pCollection = pipeline.apply(KafkaIO.<String, String>read()
                        .withBootstrapServers("34.176.255.191:9092")
                        .withTopic("test3")
                        .withKeyDeserializer(StringDeserializer.class)
                        .withValueDeserializer(StringDeserializer.class)
        );
        PCollection<Integer> wrd = pCollection.apply(
                ParDo.of(new StreamOperation()));
        //Here we are starting the pipeline
        pipeline.run();

    }
}



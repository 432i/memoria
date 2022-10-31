package org.example;


import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.util.Properties;
import java.util.Scanner;

public class App
{
    public static void main( String[] args ) {
        System.out.println( "conecting to kafka from apache-beam" );
        String machine_ip = "34.176.127.40:9092";
        Scanner scanner = new Scanner(System.in);
        System.out.println("Elige el dataset:");
        System.out.println("1.- Twitter");
        System.out.println("2.- Logs");
        System.out.println("3.- IoT");
        int dataset = scanner.nextInt();
        if (dataset == 1){
            twitter_topic_connection(machine_ip);
        } else if (dataset == 2) {
            log_topic_connection(machine_ip);
        }else {
            iot_topic_connection(machine_ip);
        }

    }
    public static void iot_topic_connection(String IP) {
        System.out.println( "Initiating connection with iot topic" );
        Pipeline pipeline = Pipeline.create();
        PCollection<KafkaRecord<String, String>> pCollectionA = pipeline.apply(KafkaIO.<String, String>read()
                .withBootstrapServers(IP)
                .withTopic("iotA")
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
        );
        PCollection<KafkaRecord<String, String>> pCollectionB = pipeline.apply(KafkaIO.<String, String>read()
                .withBootstrapServers(IP)
                .withTopic("iotB")
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
        );

        PCollection<KV<String, String>> wrdA = pCollectionA.apply(
                MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                        .via((KafkaRecord<String, String> record) -> KV.of(record.getKV().getKey(), "poto"))
                );

        wrdA.apply(
                MapElements.via(
                        new SimpleFunction<KV<String, String>, KV<String, String>>() {
                            @Override
                            public KV<String, String> apply(KV<String, String> record) {
                                System.out.println(
                                        ", llave:"+record.getKey()+", valor:"+record.getValue());
                                return record;
                            }
                        }));

        PCollection<String> wrdB = pCollectionB.apply(
                ParDo.of(new StreamOperation()));

        //Here we are starting the pipeline
        pipeline.run();
    }
    public static void twitter_topic_connection(String IP) {

    }
    public static void log_topic_connection(String IP) {

    }
    //helper methods

    //method to parse records from the different sources
    public static String splitValue(String value, Integer source){
        String[] parts = new String[0];
        if(source == 0){ //iot line separator
            //asumming an input of type 2707176363363894:2021-02-07 00:03:19,1612656199,63.3,17.4
            parts = value.split(",");
            return parts[3];
        } else if (source == 2) { //logs line separator
            //[22/Jan/2019:03:56:16 +0330] "GET /image/60844/productModel/200x200 HTTP/1.1" 402 5667 "https://www.zanbil.ir/m/filter/b113" "Mozilla/5.0 (Linux; Android 6.0; ALE-L21 Build/HuaweiALE-L21) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.158 Mobile Safari/537.36" "-
            parts = value.split("(1.1\" )|(1.0\" )|(\" (?=\\d{3}))");
            parts = parts[1].split(" ");
            //System.out.println(Arrays.toString(parts));
            return parts[0];
        }else{ //twitter line separator
            return value.substring(0, 5);
        }
    }
}



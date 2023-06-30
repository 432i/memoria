package org.example;


import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Duration;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Iterator;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class App
{
    public static void main( String[] args ) {
        System.out.println( "conecting to kafka from apache-beam" );
        String ip = get_IP();
        // params tunning for pipeline
        String[] params = {
                "--project=stone-composite-381500",
                "--gcpTempLocation=gs://dataflow_beam_test-1/temp/",
                "--runner=DataflowRunner",
                "--zone=southamerica-west1-a",
                "--region=southamerica-west1",
                //hardware conf
                "--workerMachineType=c2-standard-8",
                "--diskSizeGb=30",
                "--workerDiskType=compute.googleapis.com/projects//zones//diskTypes/pd-ssd",
                //params for performance
                "--streaming=true", //enable streaming option instead of batch
                "--numberOfWorkerHarnessThreads=20", //number of subprocess by worker
                "--numWorkers=1", //number of VM's at beggining, may change
                "--maxNumWorkers=1", //max number of VM's
        };
        PipelineOptions options =
                PipelineOptionsFactory.fromArgs(params).withValidation().create();
        //
        Scanner scanner = new Scanner(System.in);
        System.out.println("Elige el dataset:");
        System.out.println("1.- Twitter");
        System.out.println("2.- Logs");
        System.out.println("3.- IoT");
        int dataset = scanner.nextInt();
        if (dataset == 1){
            twitter_topic_connection(ip, options);
        } else if (dataset == 2) {
            log_topic_connection(ip, options);
        }else {
            iot_topic_connection(ip, options);
        }

    }
    public static String get_IP(){
        String ip_address = "";
        try {
            File myObj = new File("/home/ubuntu/ip_folder/params.txt");
            Scanner myReader = new Scanner(myObj);
            while (myReader.hasNextLine()) {
                ip_address = myReader.nextLine();
            }
            myReader.close();
        } catch (FileNotFoundException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
        return ip_address;
    }
    public static void iot_topic_connection(String IP, PipelineOptions options) {
        System.out.println( "Initiating connection with iot topic" );
        Pipeline pipeline = Pipeline.create(options);
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

        //mapping and windowing
        PCollection<KV<String, String>> wrdA = pCollectionA
                .apply(
                MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                        .via((KafkaRecord<String, String> record) -> KV.of(record.getKV().getKey(), splitValue(record.getKV().getValue(),0, 0)))
                ).apply(
                        Window.<KV<String,String>>into(FixedWindows.of(Duration.standardSeconds(30)))
                                .triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow()))
                                .withAllowedLateness(Duration.ZERO).accumulatingFiredPanes()
                );

        /*wrdA.apply( //method to print converted records
                MapElements.via(
                        new SimpleFunction<KV<String, String>,KV<String, String>>() {
                            @Override
                            public KV<String, String> apply(KV<String, String> record) {
                                System.out.println(
                                        ", 1:"+record.getKey()+", 0:"+record.getValue());
                                return record;
                            }
                        }));*/

        PCollection<KV<String, String>> wrdB = pCollectionB
                .apply(
                MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                        .via((KafkaRecord<String, String> record) -> KV.of(record.getKV().getKey(), splitValue(record.getKV().getValue(),0, 1)))
                ).apply(
                        Window.<KV<String,String>>into(FixedWindows.of(Duration.standardSeconds(30)))
                                .triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow()))
                                .withAllowedLateness(Duration.ZERO).accumulatingFiredPanes()
                );

        TupleTag<String> wrdA_tag = new TupleTag<>();
        TupleTag<String> wrdB_tag = new TupleTag<>();
        //CoGroupbyKey operation =~ join
        PCollection<KV<String, CoGbkResult>> results =
                KeyedPCollectionTuple.of(wrdA_tag, wrdA)
                        .and(wrdB_tag, wrdB)
                        .apply(CoGroupByKey.create());

        //here aggregation needs to be implemented
        PCollection<String> final_data =
                results.apply(
                        ParDo.of(
                                new DoFn<KV<String, CoGbkResult>, String>() {
                                    @ProcessElement
                                    public void processElement(ProcessContext c) {
                                        Float avg_temp = 0.0f;
                                        Integer array_lenght = 1;
                                        KV<String, CoGbkResult> e = c.element();
                                        System.out.println(e);
                                        String key = e.getKey();
                                        Iterable<String> wrdA_obj = e.getValue().getAll(wrdA_tag);
                                        Iterable<String> wrdB_obj = e.getValue().getAll(wrdB_tag);
                                        Iterator<String> wrdA_iter = wrdA_obj.iterator();
                                        Iterator<String> wrdB_iter = wrdB_obj.iterator();
                                        while( wrdA_iter.hasNext() && wrdB_iter.hasNext() ){
                                            // Process event1 and event2 data and write to c.output
                                            String v1 = wrdA_iter.next();
                                            String v2 = wrdB_iter.next();
                                            avg_temp = (Float.parseFloat(v1) + Float.parseFloat(v2));
                                            array_lenght += 1;
                                        }
                                        if(avg_temp == 0.0f){
                                            System.out.println("Unable to join event1 and event2");
                                        }else{
                                            avg_temp = avg_temp/array_lenght;
                                            c.output(String.valueOf(avg_temp)+"!!432&%$(())#"+current_id_A+"_"+current_id_B);
                                        }

                                    }
                                }));
        //write to kafka topic
        final_data.apply(KafkaIO.<Void, String>write()
                .withBootstrapServers(IP)
                .withTopic("iotOut")
                .withValueSerializer( StringSerializer.class).values());
        //Here we are starting the pipeline
        pipeline.run();
    }
    public static void twitter_topic_connection(String IP, PipelineOptions options) {
        System.out.println( "Initiating connection with twitter topic" );
        Pipeline pipeline = Pipeline.create(options);
        PCollection<KafkaRecord<String, String>> pCollectionA = pipeline.apply(KafkaIO.<String, String>read()
                .withBootstrapServers(IP)
                .withTopic("twitterA")
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
        );
        PCollection<KafkaRecord<String, String>> pCollectionB = pipeline.apply(KafkaIO.<String, String>read()
                .withBootstrapServers(IP)
                .withTopic("twitterB")
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
        );

        //mapping and windowing
        PCollection<KV<String, String>> wrdA = pCollectionA
                .apply(
                        MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                                .via((KafkaRecord<String, String> record) -> KV.of(record.getKV().getKey(), splitValue(record.getKV().getValue(),3, 0)))
                ).apply(
                        Window.<KV<String,String>>into(FixedWindows.of(Duration.standardSeconds(30)))
                                .triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow()))
                                .withAllowedLateness(Duration.ZERO).accumulatingFiredPanes()
                );

        PCollection<KV<String, String>> wrdB = pCollectionB
                .apply(
                        MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                                .via((KafkaRecord<String, String> record) -> KV.of(record.getKV().getKey(), splitValue(record.getKV().getValue(),3, 1)))
                ).apply(
                        Window.<KV<String,String>>into(FixedWindows.of(Duration.standardSeconds(30)))
                                .triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow()))
                                .withAllowedLateness(Duration.ZERO).accumulatingFiredPanes()
                );

        TupleTag<String> wrdA_tag = new TupleTag<>();
        TupleTag<String> wrdB_tag = new TupleTag<>();
        //CoGroupbyKey operation =~ join
        PCollection<KV<String, CoGbkResult>> results =
                KeyedPCollectionTuple.of(wrdA_tag, wrdA)
                        .and(wrdB_tag, wrdB)
                        .apply(CoGroupByKey.create());

        //here aggregation needs to be implemented
        PCollection<String> final_data =
                results.apply(
                        ParDo.of(
                                new DoFn<KV<String, CoGbkResult>, String>() {
                                    @ProcessElement
                                    public void processElement(ProcessContext c) {
                                        KV<String, CoGbkResult> e = c.element();
                                        String key = e.getKey();
                                        Iterable<String> wrdA_obj = e.getValue().getAll(wrdA_tag);
                                        Iterable<String> wrdB_obj = e.getValue().getAll(wrdB_tag);
                                        Iterator<String> wrdA_iter = wrdA_obj.iterator();
                                        Iterator<String> wrdB_iter = wrdB_obj.iterator();
                                        String total_concat = "";
                                        while( wrdA_iter.hasNext() && wrdB_iter.hasNext() ){
                                            // Process event1 and event2 data and write to c.output
                                            total_concat += wrdA_iter.next() + "&-/-q&" + wrdB_iter.next() + "&-/-q&";
                                        }
                                        if(total_concat.equals("")) {
                                            System.out.println("Unable to join event1 and event2");
                                        }else{
                                            String result = twitter_counter(total_concat);
                                            c.output(result+"!!432&%$(())#"+current_id_A+"_"+current_id_B);
                                        }
                                    }
                                }));
        //write to kafka topic
        final_data.apply(KafkaIO.<Void, String>write()
                .withBootstrapServers(IP)
                .withTopic("twitterOut")
                .withValueSerializer( StringSerializer.class).values());
        //Here we are starting the pipeline
        pipeline.run();
    }
    public static void log_topic_connection(String IP, PipelineOptions options) {
        System.out.println( "Initiating connection with log topic" );
        Pipeline pipeline = Pipeline.create(options);
        PCollection<KafkaRecord<String, String>> pCollectionA = pipeline.apply(KafkaIO.<String, String>read()
                .withBootstrapServers(IP)
                .withTopic("logA")
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
        );
        PCollection<KafkaRecord<String, String>> pCollectionB = pipeline.apply(KafkaIO.<String, String>read()
                .withBootstrapServers(IP)
                .withTopic("logB")
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
        );

        //mapping and windowing
        PCollection<KV<String, String>> wrdA = pCollectionA
                .apply(
                        MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                                .via((KafkaRecord<String, String> record) -> KV.of(record.getKV().getKey(), splitValue(record.getKV().getValue(),2, 0)))
                ).apply(
                        Window.<KV<String,String>>into(FixedWindows.of(Duration.standardSeconds(30)))
                                .triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow()))
                                .withAllowedLateness(Duration.ZERO).accumulatingFiredPanes()
                );

        PCollection<KV<String, String>> wrdB = pCollectionB
                .apply(
                        MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                                .via((KafkaRecord<String, String> record) -> KV.of(record.getKV().getKey(), splitValue(record.getKV().getValue(),2, 1)))
                ).apply(
                        Window.<KV<String,String>>into(FixedWindows.of(Duration.standardSeconds(30)))
                                .triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow()))
                                .withAllowedLateness(Duration.ZERO).accumulatingFiredPanes()
                );

        TupleTag<String> wrdA_tag = new TupleTag<>();
        TupleTag<String> wrdB_tag = new TupleTag<>();
        //CoGroupbyKey operation =~ join
        PCollection<KV<String, CoGbkResult>> results =
                KeyedPCollectionTuple.of(wrdA_tag, wrdA)
                        .and(wrdB_tag, wrdB)
                        .apply(CoGroupByKey.create());

        //here aggregation needs to be implemented
        PCollection<String> final_data =
                results.apply(
                        ParDo.of(
                                new DoFn<KV<String, CoGbkResult>, String>() {
                                    @ProcessElement
                                    public void processElement(ProcessContext c) {
                                        KV<String, CoGbkResult> e = c.element();
                                        String key = e.getKey();
                                        Iterable<String> wrdA_obj = e.getValue().getAll(wrdA_tag);
                                        Iterable<String> wrdB_obj = e.getValue().getAll(wrdB_tag);
                                        Iterator<String> wrdA_iter = wrdA_obj.iterator();
                                        Iterator<String> wrdB_iter = wrdB_obj.iterator();
                                        Integer total_errors = 0;
                                        while( wrdA_iter.hasNext() && wrdB_iter.hasNext() ){
                                            // Process event1 and event2 data and write to c.output
                                            String concat = wrdA_iter.next() + " " + wrdB_iter.next();
                                            Integer v1 = check_if_error(concat);
                                            total_errors += v1;
                                        }
                                        if(total_errors == 0) {
                                            System.out.println("there is no errors or something happened");
                                        }else{
                                            c.output(String.valueOf(total_errors)+"!!432&%$(())#"+current_id_A+"_"+current_id_B);
                                        }
                                    }
                                }));
        //write to kafka topic
        final_data.apply(KafkaIO.<Void, String>write()
        .withBootstrapServers(IP)
        .withTopic("iotOut")
                .withValueSerializer( StringSerializer.class).values());
        //Here we are starting the pipeline
        pipeline.run();
    }
    //helper methods
    public static String current_id_A;
    public static String current_id_B;
    //method to parse records from the different sources
    public static String splitValue(String value, Integer source, Integer from_topic){ //topic A is 0, topic B is 1
        String[] parts = new String[0];
        String msg_id = "";
        String msg_value = "";
        if(source == 0){ //iot line separator
            //asumming an input of type 2707176363363894:2021-02-07 00:03:19,1612656199,63.3,17.4,ID
            //now would be 2707176363363894:2021-02-07 00:03:19,1612656199,63.3,17.4,ID
            parts = value.split(",");
            msg_id = parts[4];
            msg_value = parts[3];
        } else if (source == 2) { //logs line separator
            //[22/Jan/2019:03:56:16 +0330] "GET /image/60844/productModel/200x200 HTTP/1.1" 402 5667 "https://www.zanbil.ir/m/filter/b113" "Mozilla/5.0 (Linux; Android 6.0; ALE-L21 Build/HuaweiALE-L21) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.158 Mobile Safari/537.36" "-
            parts = value.split("(1.1\" )|(1.0\" )|(\" (?=\\d{3}))");
            msg_id = value.split("!!432&%$(())#")[1];
            parts = parts[1].split(" ");
            //System.out.println(Arrays.toString(parts));
            msg_value = parts[0];
        }else{ //twitter line separator
            msg_id = value.split("!!432&%$(())#")[1];
            msg_value = value.substring(0, 5);
        }
        if(from_topic == 0){
            current_id_A = msg_id;
        }else{
            current_id_B = msg_id;
        }
        return msg_value;
    }
    //counts and store in variables the type of tweet received
    public static String twitter_counter(String tweets){
        int NORMAL_TWEETS = 0;
        int RE_TWEETS = 0;
        int RESPONSES = 0;
        if(!tweets.contains("null")){
            String[] parts = tweets.split("&-/-q&");
            Pattern rt_pattern = Pattern.compile("^RT");
            Pattern response_pattern = Pattern.compile("^@");
            String tweet;
            for (int i = 0; i < parts.length; i++){
                tweet = parts[i];
                rt_pattern = Pattern.compile("^RT");
                response_pattern = Pattern.compile("^@");
                Matcher rt_matcher = rt_pattern.matcher(tweet);
                Matcher response_matcher = response_pattern.matcher(tweet);
                if(rt_matcher.find()){
                    RE_TWEETS += 1;
                } else if (response_matcher.find()) {
                    RESPONSES += 1;
                }else{
                    NORMAL_TWEETS += 1;
                }
            }
        }
        return "number of normal tweets: "+NORMAL_TWEETS +", no. re tweets: "+ RE_TWEETS +", no. of responses: "+ RESPONSES;
    }

    //checks if the value is a 400-499 request code
    // and return the count of them
    public static Integer check_if_error(String value){
        int errors_number = 0;
        String[] numbers = value.split(" ");
        if (Integer.parseInt(numbers[0]) >= 400 && Integer.parseInt(numbers[0]) < 500){
            errors_number += 1;
        }
        if (Integer.parseInt(numbers[1]) >= 400 && Integer.parseInt(numbers[1]) < 500){
            errors_number += 1;
        }
        return errors_number;
    }

}



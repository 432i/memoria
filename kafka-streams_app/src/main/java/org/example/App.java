package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Hello world!
 *
 */
public class App
{
    public static void main( String[] args ) throws Exception{
        System.out.println( "Iniciando consumidor de kafka streams" );
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-test");
        String ip = get_IP();
        System.out.println("encontre la ip: "+ ip);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, ip);
        // configuration to improve latency.
        props.put(StreamsConfig.producerPrefix(ProducerConfig.BATCH_SIZE_CONFIG), 200000); //disable for all measurements except latency
        props.put(StreamsConfig.producerPrefix(ProducerConfig.LINGER_MS_CONFIG), 0); //disable buffering (storing data temporarily). disable this config for throughtput metric
       //
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        props.put(StreamsConfig.WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_CONFIG, 30000);
        Scanner scanner = new Scanner(System.in);
        System.out.println("Elige el dataset:");
        System.out.println("1.- Twitter");
        System.out.println("2.- Logs");
        System.out.println("3.- IoT");
        int dataset = scanner.nextInt();
        if (dataset == 1){
            twitter_topic_connection(props);
        } else if (dataset == 2) {
            log_topic_connection(props);
        }else {
            iot_topic_connection(props);
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

    public static void iot_topic_connection(Properties props) throws IOException {
        System.out.println( "Initiating connection with iot topic" );
        final StreamsBuilder builder = new StreamsBuilder();

        final Serde<String> stringSerde = Serdes.String();
        //final Serde<Integer> intSerde = Serdes.Integer();

        KStream<String, String> iotA = builder.stream("iotA",
                Consumed.with(stringSerde, stringSerde).withTimestampExtractor(new StreamsTimestampExtractor.myTimestampExtractor()) )
                .mapValues(value -> splitValue(value, 0));

        KStream<String, String> iotB = builder.stream("iotB",
                Consumed.with(stringSerde, stringSerde).withTimestampExtractor(new StreamsTimestampExtractor.myTimestampExtractor()) )
                .mapValues(value -> splitValue(value, 0));

        ValueJoiner<String, String, Float> valueJoiner = (leftValue, rightValue) -> Float.parseFloat(leftValue) + Float.parseFloat(rightValue);

        KStream<String, Float> combinedStream =
                iotA.join(
                        iotB,
                        valueJoiner,
                        JoinWindows.of(Duration.ofSeconds(10)),
                        StreamJoined.with(Serdes.String(), stringSerde, stringSerde));

        combinedStream.groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofSeconds(5)))
                .aggregate(() -> 0.0f,
                        (key, value, total) -> total/2 + value/2,
                        Materialized.with(Serdes.String(), Serdes.Float()))
                        //.suppress(untilWindowCloses(unbounded()))
                .toStream()
                .map((wk, value) -> KeyValue.pair(wk.key(), String.valueOf(value)))
                .to("iotOut", Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }

    public static void twitter_topic_connection(Properties props) throws IOException {
        System.out.println( "Initiating connection with twitter topic" );
        final StreamsBuilder builder = new StreamsBuilder();

        final Serde<String> stringSerde = Serdes.String();

        KStream<String, String> twitterA = builder.stream("twitterA",
                        Consumed.with(stringSerde, stringSerde).withTimestampExtractor(new StreamsTimestampExtractor.myTimestampExtractor()) )
                .mapValues(value -> splitValue(value, 1));

        KStream<String, String> twitterB = builder.stream("twitterB",
                        Consumed.with(stringSerde, stringSerde).withTimestampExtractor(new StreamsTimestampExtractor.myTimestampExtractor()) )
                .mapValues(value -> splitValue(value, 1));
        ValueJoiner<String, String, String> valueJoiner = (leftValue, rightValue) -> leftValue + "&-/-q&" + rightValue;

        KStream<String, String> combinedStream =
                twitterA.join(
                                twitterB,
                                valueJoiner,
                                JoinWindows.of(Duration.ofMinutes(10)),
                                StreamJoined.with(Serdes.String(), stringSerde, stringSerde))
                        .peek((key, value) -> System.out.println("Stream-Stream Join: record key " + key + ", value:" + value)
                        );

        System.out.println("Initiating tumbling window operation");
        combinedStream.groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofSeconds(20)))
                .aggregate(() -> "",
                        (key, value, total) -> cut_string(total) + twitter_counter(value),
                        Materialized.with(Serdes.String(), Serdes.String()))
                //.suppress(untilWindowCloses(unbounded()))
                .toStream()
                .map((wk, value) -> KeyValue.pair(wk.key(), value))
                .to("twitterOut", Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }

    public static void log_topic_connection(Properties props) throws IOException {
        System.out.println( "Initiating connection with log topic" );

        final StreamsBuilder builder = new StreamsBuilder();

        final Serde<String> stringSerde = Serdes.String();

        KStream<String, String> logA = builder.stream("logA",
                        Consumed.with(stringSerde, stringSerde).withTimestampExtractor(new StreamsTimestampExtractor.myTimestampExtractor()) )
                .mapValues(value -> splitValue(value, 2));

        KStream<String, String> logB = builder.stream("logB",
                        Consumed.with(stringSerde, stringSerde).withTimestampExtractor(new StreamsTimestampExtractor.myTimestampExtractor()) )
                .mapValues(value -> splitValue(value, 2));
        ValueJoiner<String, String, String> valueJoiner = (leftValue, rightValue) -> leftValue + " " + rightValue;

        KStream<String, String> combinedStream =
                logA.join(
                                logB,
                                valueJoiner,
                                JoinWindows.of(Duration.ofMinutes(10)),
                                StreamJoined.with(Serdes.String(), stringSerde, stringSerde));

        combinedStream.groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofSeconds(20)))
                .aggregate(() -> 0,
                        (key, value, total) -> total + check_if_error(value),
                        Materialized.with(Serdes.String(), Serdes.Integer()))
                //.suppress(untilWindowCloses(unbounded()))
                .toStream()
                .map((wk, value) -> KeyValue.pair(wk.key(), String.valueOf(value)))
                .to("logOut", Produced.with(Serdes.String(), Serdes.String()));


        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
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
    //mantains the total string in a fixed size
    public static String cut_string(String total){
        total = "";
        return total;
    }
    //counts and store in constant variables the type of tweet received
    public static int NORMAL_TWEETS = 0;
    public static int RE_TWEETS = 0;
    public static int RESPONSES = 0;
    public static String twitter_counter(String tweets){
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
        return NORMAL_TWEETS +" "+ RE_TWEETS +" "+ RESPONSES;
    }
}



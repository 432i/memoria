package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.*;

import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;
import static org.apache.kafka.streams.kstream.Suppressed.untilWindowCloses;

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
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "34.176.255.191:9092");
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

    public static void iot_topic_connection(Properties props) throws IOException {
        System.out.println( "Initiating connection with iot topic" );
        final StreamsBuilder builder = new StreamsBuilder();

        final Serde<String> stringSerde = Serdes.String();
        //final Serde<Integer> intSerde = Serdes.Integer();

        KStream<String, String> iotA = builder.stream("iotA",
                Consumed.with(stringSerde, stringSerde).withTimestampExtractor(new StreamsTimestampExtractor.myTimestampExtractor()) )
                .mapValues(value -> splitValue(value, 0))
                .peek((key, value) -> System.out.println("iotA key: " + key + " , value: " + value));;

        KStream<String, String> iotB = builder.stream("iotB",
                Consumed.with(stringSerde, stringSerde).withTimestampExtractor(new StreamsTimestampExtractor.myTimestampExtractor()) )
                .mapValues(value -> splitValue(value, 0))
                .peek((key, value) -> System.out.println("iotB key: " + key + " , value: " + value));

        System.out.println("Initiating join operation");
        ValueJoiner<String, String, Float> valueJoiner = (leftValue, rightValue) -> Float.parseFloat(leftValue) + Float.parseFloat(rightValue);

        KStream<String, Float> combinedStream =
                iotA.join(
                        iotB,
                        valueJoiner,
                        JoinWindows.of(Duration.ofMinutes(10)),
                        StreamJoined.with(Serdes.String(), stringSerde, stringSerde))
                        .peek((key, value) -> System.out.println("Stream-Stream Join: record key " + key + ", value: " + value)
                );

        System.out.println("Initiating tumbling window operation");
        combinedStream.groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofSeconds(40)))
                .aggregate(() -> 0.0f,
                        (key, value, total) -> total/2 + value/2,
                        Materialized.with(Serdes.String(), Serdes.Float()))
                        //.suppress(untilWindowCloses(unbounded()))
                .toStream()
                .map((wk, value) -> KeyValue.pair(wk.key(), value))
                .peek((key, value) -> System.out.println("FINAL WINDOW - key " +key +", average temperature in celsius: " + value));


        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }

    public static void twitter_topic_connection(Properties props) throws IOException {
        System.out.println( "Initiating connection with twitter topic" );

        final StreamsBuilder builder = new StreamsBuilder();

        final Serde<String> stringSerde = Serdes.String();

        KStream<String, String> twitterA = builder.stream("twitterA",
                        Consumed.with(stringSerde, stringSerde).withTimestampExtractor(new StreamsTimestampExtractor.myTimestampExtractor()) )
                .peek((key, value) -> System.out.println("twitterA key: " + key + " , value: " + value));

        KStream<String, String> twitterB = builder.stream("twitterB",
                        Consumed.with(stringSerde, stringSerde).withTimestampExtractor(new StreamsTimestampExtractor.myTimestampExtractor()) )
                .peek((key, value) -> System.out.println("twitterB key: " + key + " , value: " + value));
        System.out.println("Initiating join operation");
        ValueJoiner<String, String, String> valueJoiner = (leftValue, rightValue) -> leftValue + " " + rightValue;

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
                .windowedBy(TimeWindows.of(Duration.ofSeconds(15)))
                .aggregate(() -> 0,
                        (key, value, total) -> total + value.length(),
                        Materialized.with(Serdes.String(), Serdes.Integer()))
                //.suppress(untilWindowCloses(unbounded()))
                .toStream()
                .map((wk, value) -> KeyValue.pair(wk.key(), value))
                .peek((key, value) -> System.out.println("Outgoing record - key " +key +", number of characters of the joined streams: " + value));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }

    public static void log_topic_connection(Properties props) throws IOException {
        System.out.println( "Initiating connection with log topic" );

        final StreamsBuilder builder = new StreamsBuilder();

        final Serde<String> stringSerde = Serdes.String();

        KStream<String, String> logA = builder.stream("logA",
                        Consumed.with(stringSerde, stringSerde).withTimestampExtractor(new StreamsTimestampExtractor.myTimestampExtractor()) )
                .mapValues(value -> splitValue(value, 2))
                .peek((key, value) -> System.out.println("logA key: " + key + " , value: " + value));

        KStream<String, String> logB = builder.stream("logB",
                        Consumed.with(stringSerde, stringSerde).withTimestampExtractor(new StreamsTimestampExtractor.myTimestampExtractor()) )
                .mapValues(value -> splitValue(value, 2))
                .peek((key, value) -> System.out.println("logB key: " + key + " , value: " + value));
        System.out.println("Initiating join operation");
        ValueJoiner<String, String, String> valueJoiner = (leftValue, rightValue) -> leftValue + " " + rightValue;

        KStream<String, String> combinedStream =
                logA.join(
                                logB,
                                valueJoiner,
                                JoinWindows.of(Duration.ofMinutes(10)),
                                StreamJoined.with(Serdes.String(), stringSerde, stringSerde))
                        .peek((key, value) -> System.out.println("Stream-Stream Join: record key " + key + ", value:" + value)
                        );

        System.out.println("Initiating tumbling window operation");
        combinedStream.groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofSeconds(20)))
                .aggregate(() -> 0,
                        (key, value, total) -> total + check_if_error(value),
                        Materialized.with(Serdes.String(), Serdes.Integer()))
                //.suppress(untilWindowCloses(unbounded()))
                .toStream()
                .map((wk, value) -> KeyValue.pair(wk.key(),value))
                .peek((key, value) -> System.out.println("Outgoing record - key " +key +", number of characters of the joined streams: " + value));


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
            // 31.56.96.51 - - [22/Jan/2019:03:56:16 +0330] "GET /image/60844/productModel/200x200 HTTP/1.1" 200 5667 "https://www.zanbil.ir/m/filter/b113" "Mozilla/5.0 (Linux; Android 6.0; ALE-L21 Build/HuaweiALE-L21) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.158 Mobile Safari/537.36" "-"
            //hay que sacarle las "
            //[22/Jan/2019:03:56:16 +0330] GET /image/60844/productModel/200x200 HTTP/1.1 200 5667 https://www.zanbil.ir/m/filter/b113 Mozilla/5.0 (Linux; Android 6.0; ALE-L21 Build/HuaweiALE-L21) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.158 Mobile Safari/537.36 -
            String result = "";
            Pattern pattern1 = Pattern.compile("HTTP.* /d{3}");
            Pattern pattern2 = Pattern.compile(" /d{3} ");
            Matcher matcher = pattern1.matcher(value);
            if (matcher.matches()) result = matcher.group(0);
            System.out.println(result);
            Matcher matcher2 = pattern2.matcher(result);
            if (matcher2.matches()) return matcher2.group(0);

        }else{ //twitter line separator

        }
        return "";
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



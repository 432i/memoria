package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.functions.source.FromElementsFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class App
{
    public static void main( String[] args ) throws Exception {
        System.out.println( "Iniciando consumidor de flink" );
        String ip = "34.176.255.191:9092";
        Scanner scanner = new Scanner(System.in);
        System.out.println("Elige el dataset:");
        System.out.println("1.- Twitter");
        System.out.println("2.- Logs");
        System.out.println("3.- IoT");
        int dataset = scanner.nextInt();
        if (dataset == 1){
            twitter_topic_connection( ip);
        } else if (dataset == 2) {
            log_topic_connection(ip);
        }else {
            iot_topic_connection( ip);
        }
    }
    public static void iot_topic_connection( String IP) throws Exception {
        System.out.println( "Initiating connection with iot topic" );
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<ConsumerRecord> iotA = KafkaSource.<ConsumerRecord>builder()
                .setBootstrapServers(IP)
                .setTopics("iotA")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(KafkaRecordDeserializationSchema.of(new KafkaDeserializationSchema<ConsumerRecord>() {
                    @Override
                    public boolean isEndOfStream(ConsumerRecord record) {
                        return false;
                    }

                    @Override
                    public ConsumerRecord deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
                        String key = new String(record.key(), StandardCharsets.UTF_8);
                        String value = new String(record.value(), StandardCharsets.UTF_8);
                        return new ConsumerRecord(
                                record.topic(),
                                record.partition(),
                                record.offset(),
                                key,
                                value
                        );
                    }

                    @Override
                    public TypeInformation<ConsumerRecord> getProducedType() {
                        TypeInformation<ConsumerRecord> typeInfo = TypeInformation.of(ConsumerRecord.class);
                        return typeInfo;
                    }
                }))
                .build();
        KafkaSource<ConsumerRecord> iotB = KafkaSource.<ConsumerRecord>builder()
                .setBootstrapServers(IP)
                .setTopics("iotA")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(KafkaRecordDeserializationSchema.of(new KafkaDeserializationSchema<ConsumerRecord>() {
                    @Override
                    public boolean isEndOfStream(ConsumerRecord record) {
                        return false;
                    }

                    @Override
                    public ConsumerRecord deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
                        String key = new String(record.key(), StandardCharsets.UTF_8);
                        String value = new String(record.value(), StandardCharsets.UTF_8);
                        return new ConsumerRecord(
                                record.topic(),
                                record.partition(),
                                record.offset(),
                                key,
                                value
                        );
                    }

                    @Override
                    public TypeInformation<ConsumerRecord> getProducedType() {
                        TypeInformation<ConsumerRecord> typeInfo = TypeInformation.of(ConsumerRecord.class);
                        return typeInfo;
                    }
                }))
                .build();

        DataStream<ConsumerRecord> iotA_datastream = env.fromSource(iotA, WatermarkStrategy.forMonotonousTimestamps(), "Kafka Source");
        DataStream<ConsumerRecord> iotB_datastream = env.fromSource(iotB, WatermarkStrategy.forMonotonousTimestamps(), "Kafka Source");

        iotA_datastream.map(new MapFunction<ConsumerRecord, ConsumerRecord>() {
            @Override
            public ConsumerRecord map(ConsumerRecord record) throws Exception {
                String new_value = splitValue((String) record.value(), 0);
                return new ConsumerRecord(record.topic(), record.partition(), record.offset(), record.key(), new_value);
            }
        }).map((MapFunction<ConsumerRecord, String>) record -> "Value from kafka: " + record.value() + "  Key from kafka " + record.key()).print();

        iotB_datastream.map(new MapFunction<ConsumerRecord, ConsumerRecord>() {
            @Override
            public ConsumerRecord map(ConsumerRecord record) throws Exception {
                String new_value = splitValue((String) record.value(), 0);
                return new ConsumerRecord(record.topic(), record.partition(), record.offset(), record.key(), new_value);
            }
        }).map((MapFunction<ConsumerRecord, String>) record -> "Value from kafka: " + record.value() + "  Key from kafka " + record.key()).print();

        env.execute();
    }
    public static void twitter_topic_connection(String IP) throws Exception{
        System.out.println( "Initiating connection with Twitter topic" );
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<ConsumerRecord> twitterA = KafkaSource.<ConsumerRecord>builder()
                .setBootstrapServers(IP)
                .setTopics("twitterA")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(KafkaRecordDeserializationSchema.of(new KafkaDeserializationSchema<ConsumerRecord>() {
                    @Override
                    public boolean isEndOfStream(ConsumerRecord record) {
                        return false;
                    }

                    @Override
                    public ConsumerRecord deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
                        String key = new String(record.key(), StandardCharsets.UTF_8);
                        String value = new String(record.value(), StandardCharsets.UTF_8);
                        return new ConsumerRecord(
                                record.topic(),
                                record.partition(),
                                record.offset(),
                                key,
                                value
                        );
                    }

                    @Override
                    public TypeInformation<ConsumerRecord> getProducedType() {
                        TypeInformation<ConsumerRecord> typeInfo = TypeInformation.of(ConsumerRecord.class);
                        return typeInfo;
                    }
                }))
                .build();
        KafkaSource<ConsumerRecord> twitterB = KafkaSource.<ConsumerRecord>builder()
                .setBootstrapServers(IP)
                .setTopics("twitterB")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(KafkaRecordDeserializationSchema.of(new KafkaDeserializationSchema<ConsumerRecord>() {
                    @Override
                    public boolean isEndOfStream(ConsumerRecord record) {
                        return false;
                    }

                    @Override
                    public ConsumerRecord deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
                        String key = new String(record.key(), StandardCharsets.UTF_8);
                        String value = new String(record.value(), StandardCharsets.UTF_8);
                        return new ConsumerRecord(
                                record.topic(),
                                record.partition(),
                                record.offset(),
                                key,
                                value
                        );
                    }

                    @Override
                    public TypeInformation<ConsumerRecord> getProducedType() {
                        TypeInformation<ConsumerRecord> typeInfo = TypeInformation.of(ConsumerRecord.class);
                        return typeInfo;
                    }
                }))
                .build();

        DataStream<ConsumerRecord> twitterA_datastream = env.fromSource(twitterA, WatermarkStrategy.forMonotonousTimestamps(), "Kafka Source");
        DataStream<ConsumerRecord> twitterB_datastream = env.fromSource(twitterB, WatermarkStrategy.forMonotonousTimestamps(), "Kafka Source");

        twitterA_datastream.map(new MapFunction<ConsumerRecord, ConsumerRecord>() {
            @Override
            public ConsumerRecord map(ConsumerRecord record) throws Exception {
                String new_value = splitValue((String) record.value(), 3);
                return new ConsumerRecord(record.topic(), record.partition(), record.offset(), record.key(), new_value);
            }
        }).map((MapFunction<ConsumerRecord, String>) record -> "Value from kafka: " + record.value() + "  Key from kafka " + record.key()).print();

        twitterB_datastream.map(new MapFunction<ConsumerRecord, ConsumerRecord>() {
            @Override
            public ConsumerRecord map(ConsumerRecord record) throws Exception {
                String new_value = splitValue((String) record.value(), 3);
                return new ConsumerRecord(record.topic(), record.partition(), record.offset(), record.key(), new_value);
            }
        }).map((MapFunction<ConsumerRecord, String>) record -> "Value from kafka: " + record.value() + "  Key from kafka " + record.key()).print();

        env.execute();
    }
    public static void log_topic_connection(String IP) throws Exception{
        System.out.println( "Initiating connection with Log topic" );
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<ConsumerRecord> logA = KafkaSource.<ConsumerRecord>builder()
                .setBootstrapServers(IP)
                .setTopics("logA")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(KafkaRecordDeserializationSchema.of(new KafkaDeserializationSchema<ConsumerRecord>() {
                    @Override
                    public boolean isEndOfStream(ConsumerRecord record) {
                        return false;
                    }

                    @Override
                    public ConsumerRecord deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
                        String key = new String(record.key(), StandardCharsets.UTF_8);
                        String value = new String(record.value(), StandardCharsets.UTF_8);
                        return new ConsumerRecord(
                                record.topic(),
                                record.partition(),
                                record.offset(),
                                key,
                                value
                        );
                    }

                    @Override
                    public TypeInformation<ConsumerRecord> getProducedType() {
                        TypeInformation<ConsumerRecord> typeInfo = TypeInformation.of(ConsumerRecord.class);
                        return typeInfo;
                    }
                }))
                .build();
        KafkaSource<ConsumerRecord> logB = KafkaSource.<ConsumerRecord>builder()
                .setBootstrapServers(IP)
                .setTopics("logB")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(KafkaRecordDeserializationSchema.of(new KafkaDeserializationSchema<ConsumerRecord>() {
                    @Override
                    public boolean isEndOfStream(ConsumerRecord record) {
                        return false;
                    }

                    @Override
                    public ConsumerRecord deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
                        String key = new String(record.key(), StandardCharsets.UTF_8);
                        String value = new String(record.value(), StandardCharsets.UTF_8);
                        return new ConsumerRecord(
                                record.topic(),
                                record.partition(),
                                record.offset(),
                                key,
                                value
                        );
                    }

                    @Override
                    public TypeInformation<ConsumerRecord> getProducedType() {
                        TypeInformation<ConsumerRecord> typeInfo = TypeInformation.of(ConsumerRecord.class);
                        return typeInfo;
                    }
                }))
                .build();

        DataStream<ConsumerRecord> logA_datastream = env.fromSource(logA, WatermarkStrategy.forMonotonousTimestamps(), "Kafka Source");
        DataStream<ConsumerRecord> logB_datastream = env.fromSource(logB, WatermarkStrategy.forMonotonousTimestamps(), "Kafka Source");

        logA_datastream.map(new MapFunction<ConsumerRecord, ConsumerRecord>() {
            @Override
            public ConsumerRecord map(ConsumerRecord record) throws Exception {
                String new_value = splitValue((String) record.value(), 2);
                return new ConsumerRecord(record.topic(), record.partition(), record.offset(), record.key(), new_value);
            }
        }).map((MapFunction<ConsumerRecord, String>) record -> "Value from kafka: " + record.value() + "  Key from kafka " + record.key()).print();

        logB_datastream.map(new MapFunction<ConsumerRecord, ConsumerRecord>() {
            @Override
            public ConsumerRecord map(ConsumerRecord record) throws Exception {
                String new_value = splitValue((String) record.value(), 2);
                return new ConsumerRecord(record.topic(), record.partition(), record.offset(), record.key(), new_value);
            }
        }).map((MapFunction<ConsumerRecord, String>) record -> "Value from kafka: " + record.value() + "  Key from kafka " + record.key()).print();

        env.execute();
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

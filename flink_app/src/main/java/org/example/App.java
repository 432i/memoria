package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class App
{
    public static void main( String[] args ) throws Exception {
        System.out.println( "Iniciando consumidor de flink" );
        String ip = get_IP();
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
    public static void iot_topic_connection( String IP) throws Exception {
        System.out.println( "Initiating connection with iot topic" );
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<KafkaEvent> iotA = KafkaSource.<KafkaEvent>builder()
                .setBootstrapServers(IP)
                .setTopics("iotA")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(KafkaRecordDeserializationSchema.of(new KafkaDeserializationSchema<KafkaEvent>() {
                    @Override
                    public boolean isEndOfStream(KafkaEvent record) {
                        return false;
                    }
                    @Override
                    public KafkaEvent deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
                        String key = new String(record.key(), StandardCharsets.UTF_8);
                        String value = new String(record.value(), StandardCharsets.UTF_8);
                        //System.out.println(record.timestamp());
                        return new KafkaEvent(
                                key,
                                value,
                                record.topic(),
                                record.partition(),
                                record.offset(),
                                record.timestamp()
                        );
                    }
                    @Override
                    public TypeInformation<KafkaEvent> getProducedType() {
                        TypeInformation<KafkaEvent> typeInfo = TypeInformation.of(KafkaEvent.class);
                        return typeInfo;
                    }
                }))
                .build();
        KafkaSource<KafkaEvent> iotB = KafkaSource.<KafkaEvent>builder()
                .setBootstrapServers(IP)
                .setTopics("iotB")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(KafkaRecordDeserializationSchema.of(new KafkaDeserializationSchema<KafkaEvent>() {
                    @Override
                    public boolean isEndOfStream(KafkaEvent record) {
                        return false;
                    }

                    @Override
                    public KafkaEvent deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
                        String key = new String(record.key(), StandardCharsets.UTF_8);
                        String value = new String(record.value(), StandardCharsets.UTF_8);
                        return new KafkaEvent(
                                key,
                                value,
                                record.topic(),
                                record.partition(),
                                record.offset(),
                                record.timestamp()
                        );
                    }

                    @Override
                    public TypeInformation<KafkaEvent> getProducedType() {
                        TypeInformation<KafkaEvent> typeInfo = TypeInformation.of(KafkaEvent.class);
                        return typeInfo;
                    }
                }))
                .build();

        DataStream<KafkaEvent> iotA_datastream = env.fromSource(iotA,
                WatermarkStrategy.noWatermarks(), "Kafka Source");
        DataStream<KafkaEvent> iotB_datastream = env.fromSource(iotB,
                WatermarkStrategy.noWatermarks(), "Kafka Source");

        DataStream<KafkaEvent> mapped_iotA = iotA_datastream
                .map((MapFunction<KafkaEvent, KafkaEvent>) record -> {
                    String new_value = splitValue(record.value, 0,0);
                    return new KafkaEvent(record.key, new_value, record.topic, record.partition,
                            record.offset, record.timestamp);
                })
                .keyBy(record -> record.key);
        DataStream<KafkaEvent> mapped_iotB = iotB_datastream
                .map((MapFunction<KafkaEvent, KafkaEvent>) record -> {
                    String new_value = splitValue(record.value, 0, 1);
                    return new KafkaEvent(record.key, new_value, record.topic, record.partition,
                            record.offset, record.timestamp);
                })
                .keyBy(record -> record.key);

        DataStream<String> joined_streams = mapped_iotA
                .join(mapped_iotB)
                .where(new KeySelector<KafkaEvent, String>() {
                    @Override
                    public String getKey(KafkaEvent record) throws Exception {
                        //System.out.println("key,value1 : "+record.key+" "+record.value);
                        return (String)  record.key;
                    }
                })
                .equalTo(new KeySelector<KafkaEvent, String>() {
                    @Override
                    public String getKey(KafkaEvent record) throws Exception {
                        //System.out.println("key,value2 : "+ record.key+" "+record.value);
                        return (String) record.key;
                    }
                })
                .window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
                .apply(new JoinFunction<KafkaEvent, KafkaEvent, String> (){
                    @Override
                    public String join(KafkaEvent record1, KafkaEvent record2) throws Exception {
                        Float sum = Float.parseFloat(record1.value) + Float.parseFloat(record2.value);
                        return String.valueOf(sum/2)+"!!432&%$(())#"+current_id_A+"_"+current_id_B;
                    }
                });
        //joined_streams.print();
        //write into kafka

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(IP)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic("iotOut")
                .setValueSerializationSchema(new SimpleStringSchema())
                .build()
                )
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        joined_streams.sinkTo(sink);

        env.execute();
    }
    public static void twitter_topic_connection(String IP) throws Exception{
        System.out.println( "Initiating connection with Twitter topic" );
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<KafkaEvent> twitterA = KafkaSource.<KafkaEvent>builder()
                .setBootstrapServers(IP)
                .setTopics("twitterA")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(KafkaRecordDeserializationSchema.of(new KafkaDeserializationSchema<KafkaEvent>() {
                    @Override
                    public boolean isEndOfStream(KafkaEvent record) {
                        return false;
                    }
                    @Override
                    public KafkaEvent deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
                        String key = new String(record.key(), StandardCharsets.UTF_8);
                        String value = new String(record.value(), StandardCharsets.UTF_8);
                        //System.out.println(record.timestamp());
                        return new KafkaEvent(
                                key,
                                value,
                                record.topic(),
                                record.partition(),
                                record.offset(),
                                record.timestamp()
                        );
                    }
                    @Override
                    public TypeInformation<KafkaEvent> getProducedType() {
                        TypeInformation<KafkaEvent> typeInfo = TypeInformation.of(KafkaEvent.class);
                        return typeInfo;
                    }
                }))
                .build();
        KafkaSource<KafkaEvent> twitterB = KafkaSource.<KafkaEvent>builder()
                .setBootstrapServers(IP)
                .setTopics("twitterB")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(KafkaRecordDeserializationSchema.of(new KafkaDeserializationSchema<KafkaEvent>() {
                    @Override
                    public boolean isEndOfStream(KafkaEvent record) {
                        return false;
                    }

                    @Override
                    public KafkaEvent deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
                        String key = new String(record.key(), StandardCharsets.UTF_8);
                        String value = new String(record.value(), StandardCharsets.UTF_8);
                        return new KafkaEvent(
                                key,
                                value,
                                record.topic(),
                                record.partition(),
                                record.offset(),
                                record.timestamp()
                        );
                    }

                    @Override
                    public TypeInformation<KafkaEvent> getProducedType() {
                        TypeInformation<KafkaEvent> typeInfo = TypeInformation.of(KafkaEvent.class);
                        return typeInfo;
                    }
                }))
                .build();

        DataStream<KafkaEvent> twitterA_datastream = env.fromSource(twitterA,
                WatermarkStrategy.noWatermarks(), "Kafka Source");
        DataStream<KafkaEvent> twitterB_datastream = env.fromSource(twitterB,
                WatermarkStrategy.noWatermarks(), "Kafka Source");

        DataStream<KafkaEvent> mapped_twitterA = twitterA_datastream
                .map((MapFunction<KafkaEvent, KafkaEvent>) record -> {
                    String new_value = splitValue(record.value, 3, 0);
                    return new KafkaEvent(record.key, new_value, record.topic, record.partition,
                            record.offset, record.timestamp);
                })
                .keyBy(record -> record.key);
        DataStream<KafkaEvent> mapped_twitterB = twitterB_datastream
                .map((MapFunction<KafkaEvent, KafkaEvent>) record -> {
                    String new_value = splitValue(record.value, 3, 1);
                    return new KafkaEvent(record.key, new_value, record.topic, record.partition,
                            record.offset, record.timestamp);
                })
                .keyBy(record -> record.key);

        DataStream<String> joined_streams = mapped_twitterA
                .join(mapped_twitterB)
                .where(new KeySelector<KafkaEvent, String>() {
                    @Override
                    public String getKey(KafkaEvent record) throws Exception {
                        //System.out.println("key,value1 : "+record.key+" "+record.value);
                        return (String)  record.key;
                    }
                })
                .equalTo(new KeySelector<KafkaEvent, String>() {
                    @Override
                    public String getKey(KafkaEvent record) throws Exception {
                        //System.out.println("key,value2 : "+ record.key+" "+record.value);
                        return (String) record.key;
                    }
                })
                .window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
                .apply(new JoinFunction<KafkaEvent, KafkaEvent, String> (){
                    @Override
                    public String join(KafkaEvent record1, KafkaEvent record2) throws Exception {
                        String twitter_counter = record1.value +"&-/-q&"+ record2.value;
                        twitter_counter = twitter_counter(twitter_counter);
                        return twitter_counter+"!!432&%$(())#"+current_id_A+"_"+current_id_B;
                    }
                });
        //joined_streams.print();
        //write into kafka

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(IP)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("twitterOut")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        joined_streams.sinkTo(sink);

        env.execute();
    }
    public static void log_topic_connection(String IP) throws Exception{
        System.out.println( "Initiating connection with Log topic" );
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<KafkaEvent> logA = KafkaSource.<KafkaEvent>builder()
                .setBootstrapServers(IP)
                .setTopics("logA")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(KafkaRecordDeserializationSchema.of(new KafkaDeserializationSchema<KafkaEvent>() {
                    @Override
                    public boolean isEndOfStream(KafkaEvent record) {
                        return false;
                    }
                    @Override
                    public KafkaEvent deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
                        String key = new String(record.key(), StandardCharsets.UTF_8);
                        String value = new String(record.value(), StandardCharsets.UTF_8);
                        //System.out.println(record.timestamp());
                        return new KafkaEvent(
                                key,
                                value,
                                record.topic(),
                                record.partition(),
                                record.offset(),
                                record.timestamp()
                        );
                    }
                    @Override
                    public TypeInformation<KafkaEvent> getProducedType() {
                        TypeInformation<KafkaEvent> typeInfo = TypeInformation.of(KafkaEvent.class);
                        return typeInfo;
                    }
                }))
                .build();
        KafkaSource<KafkaEvent> logB = KafkaSource.<KafkaEvent>builder()
                .setBootstrapServers(IP)
                .setTopics("logB")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(KafkaRecordDeserializationSchema.of(new KafkaDeserializationSchema<KafkaEvent>() {
                    @Override
                    public boolean isEndOfStream(KafkaEvent record) {
                        return false;
                    }

                    @Override
                    public KafkaEvent deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
                        String key = new String(record.key(), StandardCharsets.UTF_8);
                        String value = new String(record.value(), StandardCharsets.UTF_8);
                        return new KafkaEvent(
                                key,
                                value,
                                record.topic(),
                                record.partition(),
                                record.offset(),
                                record.timestamp()
                        );
                    }

                    @Override
                    public TypeInformation<KafkaEvent> getProducedType() {
                        TypeInformation<KafkaEvent> typeInfo = TypeInformation.of(KafkaEvent.class);
                        return typeInfo;
                    }
                }))
                .build();

        DataStream<KafkaEvent> logA_datastream = env.fromSource(logA,
                WatermarkStrategy.noWatermarks(), "Kafka Source");
        DataStream<KafkaEvent> logB_datastream = env.fromSource(logB,
                WatermarkStrategy.noWatermarks(), "Kafka Source");

        DataStream<KafkaEvent> mapped_logA = logA_datastream
                .map((MapFunction<KafkaEvent, KafkaEvent>) record -> {
                    String new_value = splitValue(record.value, 2, 0);
                    return new KafkaEvent(record.key, new_value, record.topic, record.partition,
                            record.offset, record.timestamp);
                })
                .keyBy(record -> record.key);
        DataStream<KafkaEvent> mapped_logB = logB_datastream
                .map((MapFunction<KafkaEvent, KafkaEvent>) record -> {
                    String new_value = splitValue(record.value, 2, 1);
                    return new KafkaEvent(record.key, new_value, record.topic, record.partition,
                            record.offset, record.timestamp);
                })
                .keyBy(record -> record.key);

        DataStream<String> joined_streams = mapped_logA
                .join(mapped_logB)
                .where(new KeySelector<KafkaEvent, String>() {
                    @Override
                    public String getKey(KafkaEvent record) throws Exception {
                        //System.out.println("key,value1 : "+record.key+" "+record.value);
                        return (String)  record.key;
                    }
                })
                .equalTo(new KeySelector<KafkaEvent, String>() {
                    @Override
                    public String getKey(KafkaEvent record) throws Exception {
                        //System.out.println("key,value2 : "+ record.key+" "+record.value);
                        return (String) record.key;
                    }
                })
                .window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
                .apply(new JoinFunction<KafkaEvent, KafkaEvent, String> (){
                    @Override
                    public String join(KafkaEvent record1, KafkaEvent record2) throws Exception {
                        String log_strs = record1.value +" "+ record2.value;
                        Integer errors = check_if_error(log_strs);
                        return Integer.toString(errors)+"!!432&%$(())#"+current_id_A+"_"+current_id_B;
                    }
                });
        //joined_streams.print();
        //write into kafka

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(IP)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("logOut")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        joined_streams.sinkTo(sink);

        env.execute();
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
    //checks if the value is a 400-499 request code
    // and return the count of them
    public static int errors_number = 0;
    public static Integer check_if_error(String value){
        String[] numbers = value.split(" ");
        if (Integer.parseInt(numbers[0]) >= 400 && Integer.parseInt(numbers[0]) < 500){
            errors_number += 1;
        }
        if (Integer.parseInt(numbers[1]) >= 400 && Integer.parseInt(numbers[1]) < 500){
            errors_number += 1;
        }
        return errors_number;
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

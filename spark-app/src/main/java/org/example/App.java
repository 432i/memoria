package org.example;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.streaming.Trigger;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.spark.sql.functions.*;


public class App {
    public static void main(String[] args) throws Exception {
        System.out.println("Iniciando consumidor de apache spark");
        String ip = get_IP();
        Scanner scanner = new Scanner(System.in);
        System.out.println("Elige el dataset:");
        System.out.println("1.- Twitter");
        System.out.println("2.- Logs");
        System.out.println("3.- IoT");
        int dataset = scanner.nextInt();
        if (dataset == 1) {
            twitter_topic_connection(ip);
        } else if (dataset == 2) {
            log_topic_connection(ip);
        } else {
            iot_topic_connection(ip);
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

    public static void iot_topic_connection(String IP) throws Exception {
        System.out.println( "Initiating connection with iot topic" );
        SparkSession spark = SparkSession
                .builder()
                .appName("App")
                .master("local")
                .getOrCreate();
        spark.conf().set("spark.sql.shuffle.partitions",2);
        spark.sparkContext().setLogLevel("ERROR");
        Dataset<Row> iotA = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", IP)
                .option("subscribe", "iotA")
                .load();
        iotA = iotA.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)",
                "CAST(topic AS STRING)", "CAST(timestamp AS TIMESTAMP)");
        iotA = iotA.withColumnRenamed("key", "keyA");
        iotA = iotA.withColumnRenamed("value", "valueA");
        iotA = iotA.withColumnRenamed("topic", "topicA");
        iotA = iotA.withColumnRenamed("timestamp", "timestampA");
        Dataset<Row> iotB = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", IP)
                .option("subscribe", "iotB")
                .load();
        iotB = iotB.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)",
                "CAST(topic AS STRING)", "CAST(timestamp AS TIMESTAMP)");
        iotB = iotB.withColumnRenamed("key", "keyB");
        iotB = iotB.withColumnRenamed("value", "valueB");
        iotB = iotB.withColumnRenamed("topic", "topicB");
        iotB = iotB.withColumnRenamed("timestamp", "timestampB");


        Dataset<Row> mapped_iotA = iotA.map((MapFunction<Row, Row>) row ->
        {return RowFactory.create(row.get(0), splitValue((String) row.get(1), 0), row.get(2), row.get(3));},
                RowEncoder.apply(iotA.schema()));
        mapped_iotA = mapped_iotA.selectExpr("CAST(keyA AS STRING)", "CAST(valueA AS FLOAT)",
                "CAST(topicA AS STRING)", "CAST(timestampA AS TIMESTAMP)");
        Dataset<Row> mapped_iotB = iotB.map((MapFunction<Row, Row>) row ->
        {return RowFactory.create(row.get(0), splitValue((String) row.get(1), 0), row.get(2), row.get(3));},
                RowEncoder.apply(iotB.schema()));
        mapped_iotB = mapped_iotB.selectExpr("CAST(keyB AS STRING)", "CAST(valueB AS FLOAT)",
                "CAST(topicB AS STRING)", "CAST(timestampB AS TIMESTAMP)");

        Dataset<Row> joined_streams = mapped_iotA
                .join(mapped_iotB, mapped_iotA.col("keyA").equalTo(mapped_iotB.col("keyB")));

        joined_streams = joined_streams .map(
                (MapFunction<Row, Row>) row ->
                {return RowFactory.create(row.get(0), avrg((Float)row.get(1), (Float)row.get(5)), row.get(2), row.get(3), row.get(4), row.get(5), row.get(6), row.get(7));},
                RowEncoder.apply(joined_streams.schema())
        );
        joined_streams = joined_streams.withWatermark("timestampA", "1 minute");

        Dataset<Row> windowedAvg = joined_streams.groupBy(
                functions.window(col("timestampA"), "15 seconds"),
                col("keyA")
        ).avg("valueA");
        windowedAvg = windowedAvg.withColumnRenamed("avg(valueA)", "value");
        windowedAvg = windowedAvg.withColumnRenamed("keyA", "key");

        windowedAvg
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        .writeStream()
        .trigger(Trigger.ProcessingTime("1 minutes"))
        .format("kafka")
        .format("console")
        .outputMode("append")
        .option("kafka.bootstrap.servers", IP)
        .option("topic", "iotOUT")
        .option("checkpointLocation", "/home/ubuntu/iot_spark")
        .start();
        //joined_streams.writeStream()
        //        .outputMode("append")
        //        .format("console")
        //        .start().awaitTermination();
        spark.streams().awaitAnyTermination();
    }

    public static void twitter_topic_connection(String IP) throws Exception {
        System.out.println( "Initiating connection with twitter topic" );
        SparkSession spark = SparkSession
                .builder()
                .appName("App")
                .master("local")
                .getOrCreate();
        spark.conf().set("spark.sql.shuffle.partitions",2);
        spark.sparkContext().setLogLevel("ERROR");
        Dataset<Row> twitterA = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", IP)
                .option("subscribe", "twitterA")
                .load();
        twitterA = twitterA.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)",
                "CAST(topic AS STRING)", "CAST(timestamp AS TIMESTAMP)");
        twitterA = twitterA.withColumnRenamed("key", "keyA");
        twitterA = twitterA.withColumnRenamed("value", "valueA");
        twitterA = twitterA.withColumnRenamed("topic", "topicA");
        twitterA = twitterA.withColumnRenamed("timestamp", "timestampA");
        Dataset<Row> twitterB = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", IP)
                .option("subscribe", "twitterB")
                .load();
        twitterB = twitterB.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)",
                "CAST(topic AS STRING)", "CAST(timestamp AS TIMESTAMP)");
        twitterB = twitterB.withColumnRenamed("key", "keyB");
        twitterB = twitterB.withColumnRenamed("value", "valueB");
        twitterB = twitterB.withColumnRenamed("topic", "topicB");
        twitterB = twitterB.withColumnRenamed("timestamp", "timestampB");


        Dataset<Row> mapped_twitterA = twitterA.map((MapFunction<Row, Row>) row ->
                {return RowFactory.create(row.get(0), splitValue((String) row.get(1), 3), row.get(2), row.get(3));},
                RowEncoder.apply(twitterA.schema()));
        mapped_twitterA = mapped_twitterA.selectExpr("CAST(keyA AS STRING)", "CAST(valueA AS STRING)",
                "CAST(topicA AS STRING)", "CAST(timestampA AS TIMESTAMP)");
        Dataset<Row> mapped_twitterB = twitterB.map((MapFunction<Row, Row>) row ->
                {return RowFactory.create(row.get(0), splitValue((String) row.get(1), 3), row.get(2), row.get(3));},
                RowEncoder.apply(twitterB.schema()));
        mapped_twitterB = mapped_twitterB.selectExpr("CAST(keyB AS STRING)", "CAST(valueB AS STRING)",
                "CAST(topicB AS STRING)", "CAST(timestampB AS TIMESTAMP)");

        Dataset<Row> joined_streams = mapped_twitterA
                .join(mapped_twitterB, mapped_twitterA.col("keyA").equalTo(mapped_twitterB.col("keyB")));

        joined_streams = joined_streams .map(
                (MapFunction<Row, Row>) row ->
                {return RowFactory.create(row.get(0), twitter_counter((String)row.get(1) +"&-/-q&"+ (String)row.get(5)), row.get(2), row.get(3), row.get(4), row.get(5), row.get(6), row.get(7));},
                RowEncoder.apply(joined_streams.schema())
        );
        joined_streams = joined_streams.withWatermark("timestampA", "5 seconds");

        Dataset<Row> windowedAvg = joined_streams.groupBy(
                functions.window(col("timestampA"), "15 seconds"),
                col("keyA")
        ).df();

        windowedAvg = windowedAvg.withColumnRenamed("valueA", "value");
        windowedAvg = windowedAvg.withColumnRenamed("keyA", "key");

        windowedAvg
                .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
                .writeStream()
                .trigger(Trigger.ProcessingTime("1 minutes"))
                .format("kafka")
                .outputMode("append")
                .option("kafka.bootstrap.servers", IP)
                .option("topic", "twitterOUT")
                .option("checkpointLocation", "/home/ubuntu/twitter_spark")
                .start();

        spark.streams().awaitAnyTermination();
    }

    public static void log_topic_connection(String IP) throws Exception {
        System.out.println( "Initiating connection with log topic" );
        SparkSession spark = SparkSession
                .builder()
                .appName("App")
                .master("local")
                .getOrCreate();
        spark.conf().set("spark.sql.shuffle.partitions", 2);
        spark.sparkContext().setLogLevel("ERROR");
        Dataset<Row> logA = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", IP)
                .option("subscribe", "logA")
                .load();
        logA = logA.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)",
                "CAST(topic AS STRING)", "CAST(timestamp AS TIMESTAMP)");
        logA = logA.withColumnRenamed("key", "keyA");
        logA = logA.withColumnRenamed("value", "valueA");
        logA = logA.withColumnRenamed("topic", "topicA");
        logA = logA.withColumnRenamed("timestamp", "timestampA");
        Dataset<Row> logB = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", IP)
                .option("subscribe", "logB")
                .load();
        logB = logB.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)",
                "CAST(topic AS STRING)", "CAST(timestamp AS TIMESTAMP)");
        logB = logB.withColumnRenamed("key", "keyB");
        logB = logB.withColumnRenamed("value", "valueB");
        logB = logB.withColumnRenamed("topic", "topicB");
        logB = logB.withColumnRenamed("timestamp", "timestampB");


        Dataset<Row> mapped_logA = logA.map((MapFunction<Row, Row>) row ->
                {return RowFactory.create(row.get(0), splitValue((String) row.get(1), 2), row.get(2), row.get(3));},
                RowEncoder.apply(logA.schema()));
        mapped_logA = mapped_logA.selectExpr("CAST(keyA AS STRING)", "CAST(valueA AS STRING)",
                "CAST(topicA AS STRING)", "CAST(timestampA AS TIMESTAMP)");
        Dataset<Row> mapped_logB = logB.map((MapFunction<Row, Row>) row ->
                {return RowFactory.create(row.get(0), splitValue((String) row.get(1), 2), row.get(2), row.get(3));},
                RowEncoder.apply(logB.schema()));
        mapped_logB = mapped_logB.selectExpr("CAST(keyB AS STRING)", "CAST(valueB AS STRING)",
                "CAST(topicB AS STRING)", "CAST(timestampB AS TIMESTAMP)");

        Dataset<Row> joined_streams = mapped_logA
                .join(mapped_logB, mapped_logA.col("keyA").equalTo(mapped_logB.col("keyB")));

        joined_streams = joined_streams .map(
                (MapFunction<Row, Row>) row ->
                {return RowFactory.create(row.get(0), check_if_error((String)row.get(1) +" "+ (String)row.get(5)), row.get(2), row.get(3), row.get(4), row.get(5), row.get(6), row.get(7));},
                RowEncoder.apply(joined_streams.schema())
        );
        joined_streams = joined_streams.withWatermark("timestampA", "10 seconds");

        Dataset<Row> windowedAvg = joined_streams.groupBy(
                functions.window(col("timestampA"), "15 seconds"),
                col("keyA")
        ).df();
        windowedAvg = windowedAvg.withColumnRenamed("valueA", "value");
        windowedAvg = windowedAvg.withColumnRenamed("keyA", "key");

        windowedAvg
                .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
                .writeStream()
                .trigger(Trigger.ProcessingTime("1 minutes"))
                .format("kafka")
                .outputMode("append")
                .option("kafka.bootstrap.servers", IP)
                .option("topic", "logOUT")
                .option("checkpointLocation", "/home/ubuntu/log_spark")
                .start();
        //joined_streams.writeStream()
        //        .outputMode("append")
        //        .format("console")
        //        .start().awaitTermination();
        spark.streams().awaitAnyTermination();
    }

    //helper methods
    public static Float avrg(Float num1, Float num2){
        return (num1 + num2) / 2;
    }

    //checks if the value is a 400-499 request code
    // and return the count of them
    public static int ERROR_NUMBER = 0;
    public static String check_if_error(String value){
        //System.out.println("en check if error: "+value);
        String[] numbers = value.split(" ");
        if (Integer.parseInt(numbers[0]) >= 400 && Integer.parseInt(numbers[0]) < 500){
            ERROR_NUMBER += 1;
        }
        if (Integer.parseInt(numbers[1]) >= 400 && Integer.parseInt(numbers[1]) < 500){
            ERROR_NUMBER += 1;
        }
        return String.valueOf(ERROR_NUMBER);
    }
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
            //System.out.println(parts[0]);
            return parts[0];
        }else{ //twitter line separator
            //System.out.println(value.substring(0, 5));
            return value.substring(0, 5);
        }
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
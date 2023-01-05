package org.example;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;

import java.util.Map;
import java.util.Scanner;

import static org.apache.spark.sql.functions.*;


public class App {
    public static void main(String[] args) throws Exception {
        System.out.println("Iniciando consumidor de apache spark");
        String ip = "34.176.50.58:9092";
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
        .format("kafka")
        .format("console")
        .outputMode("append")
        .option("kafka.bootstrap.servers", IP)
        .option("topic", "iot_output")
        .option("checkpointLocation", "C:\\Users\\aevi1\\Downloads")
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
        mapped_twitterA = mapped_twitterA.selectExpr("CAST(keyA AS STRING)", "CAST(valueA AS FLOAT)",
                "CAST(topicA AS STRING)", "CAST(timestampA AS TIMESTAMP)");
        Dataset<Row> mapped_twitterB = twitterB.map((MapFunction<Row, Row>) row ->
                {return RowFactory.create(row.get(0), splitValue((String) row.get(1), 3), row.get(2), row.get(3));},
                RowEncoder.apply(twitterB.schema()));
        mapped_twitterB = mapped_twitterB.selectExpr("CAST(keyB AS STRING)", "CAST(valueB AS FLOAT)",
                "CAST(topicB AS STRING)", "CAST(timestampB AS TIMESTAMP)");

        Dataset<Row> joined_streams = mapped_twitterA
                .join(mapped_twitterB, mapped_twitterA.col("keyA").equalTo(mapped_twitterB.col("keyB")));

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
                .format("kafka")
                .format("console")
                .outputMode("append")
                .option("kafka.bootstrap.servers", IP)
                .option("topic", "iot_output")
                .option("checkpointLocation", "C:\\Users\\aevi1\\Downloads")
                .start();
        joined_streams.writeStream()
                .outputMode("append")
                .format("console")
                .start().awaitTermination();
        spark.streams().awaitAnyTermination();
    }

    public static void log_topic_connection(String IP) throws Exception {
        System.out.println( "Initiating connection with log topic" );
        SparkSession spark = SparkSession
                .builder()
                .appName("App")
                .master("local")
                .getOrCreate();
        spark.conf().set("spark.sql.shuffle.partitions",2);
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
        mapped_logA = mapped_logA.selectExpr("CAST(keyA AS STRING)", "CAST(valueA AS FLOAT)",
                "CAST(topicA AS STRING)", "CAST(timestampA AS TIMESTAMP)");
        Dataset<Row> mapped_logB = logB.map((MapFunction<Row, Row>) row ->
                {return RowFactory.create(row.get(0), splitValue((String) row.get(1), 2), row.get(2), row.get(3));},
                RowEncoder.apply(logB.schema()));
        mapped_logB = mapped_logB.selectExpr("CAST(keyB AS STRING)", "CAST(valueB AS FLOAT)",
                "CAST(topicB AS STRING)", "CAST(timestampB AS TIMESTAMP)");

        Dataset<Row> joined_streams = mapped_logA
                .join(mapped_logB, mapped_logA.col("keyA").equalTo(mapped_logB.col("keyB")));

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
                .format("kafka")
                .format("console")
                .outputMode("append")
                .option("kafka.bootstrap.servers", IP)
                .option("topic", "iot_output")
                .option("checkpointLocation", "C:\\Users\\aevi1\\Downloads")
                .start();
        joined_streams.writeStream()
                .outputMode("append")
                .format("console")
                .start().awaitTermination();
        spark.streams().awaitAnyTermination();
    }

    //helper methods
    public static Float avrg(Float num1, Float num2){
        return (num1 + num2) / 2;
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
            //System.out.println(Arrays.toString(parts));
            return parts[0];
        }else{ //twitter line separator
            return value.substring(0, 5);
        }
    }
}
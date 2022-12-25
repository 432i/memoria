package org.example;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.streaming.StreamingQuery;
import java.util.Scanner;

import static org.apache.spark.sql.functions.expr;


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
        Dataset<Row> mapped_iotB = iotB.map((MapFunction<Row, Row>) row ->
        {return RowFactory.create(row.get(0), splitValue((String) row.get(1), 0), row.get(2), row.get(3));},
                RowEncoder.apply(iotB.schema()));

        //Dataset<Row> mapped_iotA_watermark = mapped_iotA.withWatermark("timestamp", "2 hours");
        //Dataset<Row> mapped_iotB_watermark = mapped_iotB.withWatermark("timestamp", "3 hours");

        Dataset<Row> joined_streams = mapped_iotA.join(mapped_iotB, expr(
                "keyA = keyB AND " +
                        "timestampA >= timestampB AND " +
                        "timestampA <= timestampB + interval 20 second ")
        );   // key is common in both DataFrames

        //1:2021-02-07 00:03:19,1612656199,63.3,17.1
        //1:2021-02-07 00:03:19,1612656199,63.3,17.2
        // Start running the query that prints the data getting from Kafka 'iotA' topic
        StreamingQuery query = iotA.writeStream()
                .outputMode("append")
                .format("console")
                .start();
        mapped_iotA.writeStream()
                .outputMode("append")
                .format("console")
                .start();
        mapped_iotB.writeStream()
                .outputMode("append")
                .format("console")
                .start();

        joined_streams.writeStream()
                .outputMode("append")
                .format("console")
                .start();

        //Getting the data value as String
        query.awaitTermination();
    }

    public static void twitter_topic_connection(String IP) throws Exception {

    }

    public static void log_topic_connection(String IP) throws Exception {

    }

    //helper methods

    //method to parse records from the different sources
    public static String splitValue(String value, Integer source){
        System.out.println(value);
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
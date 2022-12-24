package org.example;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.Scanner;
import java.util.concurrent.TimeoutException;



public class App {
    public static void main(String[] args) throws Exception {
        System.out.println("Iniciando consumidor de apache spark");
        String ip = "34.176.127.40:9092";
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
        SparkSession spark = SparkSession
                .builder()
                .appName("App")
                .master("local")
                .getOrCreate();

        Dataset<Row> iotA = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", IP)
                .option("subscribe", "iotA")
                .load();
        iotA.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");
        Dataset<Row> iotB = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", IP)
                .option("subscribe", "iotB")
                .load();
        iotB.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");

        iotA.map((MapFunction<Row, Row>) row -> {System.out.println(row.get(0)+" "+row.get(1)+" "+row.get(2)+" "+row.get(3));
            return row;
        }, RowEncoder.apply(iotA.schema()));
        Dataset<Row> mapped_iotA = iotA.map((MapFunction<Row, Row>) row -> {return RowFactory.create(row.get(0), splitValue((String) row.get(1), 0));
        }, RowEncoder.apply(iotA.schema()));
        Dataset<Row> mapped_iotB = iotB.map((MapFunction<Row, Row>) row -> {return RowFactory.create(row.get(0), splitValue((String) row.get(1), 0));
        }, RowEncoder.apply(iotA.schema()));

        Dataset<Row> mapped_iotA_watermark = mapped_iotA.withWatermark("key", "2 hours");
        Dataset<Row> mapped_iotB_watermark = mapped_iotB.withWatermark("key", "3 hours");

        // Start running the query that prints the data getting from Kafka 'iotA' topic
        StreamingQuery query = iotA.writeStream()
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
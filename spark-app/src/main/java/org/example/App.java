package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import java.util.concurrent.TimeoutException;



public class App 
{
    public static void main( String[] args ) throws TimeoutException, StreamingQueryException {
        System.out.println( "conecting to kafka from spark..." );
        SparkSession spark = SparkSession
                .builder()
                .appName("App")
                .master("local")
                .getOrCreate();

        Dataset<Row> ds = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "34.176.255.191:9092")
                .option("subscribe", "test3")
                .load();
        ds.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");
        // Start running the query that prints the data getting from Kafka 'test3' topic
        StreamingQuery query = ds.writeStream()
                .outputMode("append")
                .format("console")
                .start();

        //Getting the data value as String
        query.awaitTermination();

    }
}

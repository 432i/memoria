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
        SparkSession spark = SparkSession
                .builder()
                .appName("App")
                .config("spark.sql.shuffle.partitions", 2)
                .config("spark.sql.streaming.minBatchesToRetain", 2)
                .config("spark.driver.cores", 2)
                .config("spark.driver.memory", "6g")
                .config("spark.executor.instances", 5)
                .config("spark.executor.memory", "17g")
                .config("spark.executor.cores", 4)
                .config("spark.locality.wait", "100ms")
                .config("spark.default.parallelism", 20)
                .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC")
                .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC")
                .config("spark.executor.extraJavaOptions", "-XX:ConcGCThreads=2")
                .config("spark.executor.extraJavaOptions", "-XX:ParallelGCThreads=4")
                .config("spark.sql.autoBroadcastJoinThreshold", -1)
                .master("local")
                .getOrCreate();
        //spark.conf().set("spark.sql.shuffle.partitions",2);
        spark.sparkContext().setLogLevel("WARN");

        System.out.println("Elige el dataset:");
        System.out.println("1.- Twitter");
        System.out.println("2.- Logs");
        System.out.println("3.- IoT");
        int dataset = scanner.nextInt();
        if (dataset == 1) {
            twitter_topic_connection(ip, spark);
        } else if (dataset == 2) {
            log_topic_connection(ip, spark);
        } else {
            iot_topic_connection(ip, spark);
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

    public static void iot_topic_connection(String IP, SparkSession spark) throws Exception {
        System.out.println( "Initiating connection with iot topic" );
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
        iotA = iotA.withColumn("idA", lit(""));

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
        iotB = iotB.withColumn("idB", lit(""));

        Dataset<Row> mapped_iotA = iotA.map((MapFunction<Row, Row>) row ->
        {return RowFactory.create(row.get(0), splitValue((String) row.get(1), 0), row.get(2), row.get(3), get_id((String) row.get(1), 0));},
                RowEncoder.apply(iotA.schema()));
        mapped_iotA = mapped_iotA.selectExpr("CAST(keyA AS STRING)", "CAST(valueA AS FLOAT)",
                "CAST(topicA AS STRING)", "CAST(timestampA AS TIMESTAMP)", "CAST(idA AS STRING)");
        Dataset<Row> mapped_iotB = iotB.map((MapFunction<Row, Row>) row ->
        {return RowFactory.create(row.get(0), splitValue((String) row.get(1), 0), row.get(2), row.get(3), get_id((String) row.get(1), 0));},
                RowEncoder.apply(iotB.schema()));
        mapped_iotB = mapped_iotB.selectExpr("CAST(keyB AS STRING)", "CAST(valueB AS FLOAT)",
                "CAST(topicB AS STRING)", "CAST(timestampB AS TIMESTAMP)", "CAST(idB AS STRING)");

        Dataset<Row> joined_streams = mapped_iotA
                .join(mapped_iotB, mapped_iotA.col("keyA").equalTo(mapped_iotB.col("keyB")));

        //row_schema: keyA, avrgValue, topicA, timestampA, concatIds, keyB, valueB, topicB, timestampB, idB
        joined_streams = joined_streams.map(
                (MapFunction<Row, Row>) row ->
                {return RowFactory.create(row.get(0), avrg((Float)row.get(1), (Float)row.get(6)), row.get(2), row.get(3),
                        "!!432&%$(())#" + row.get(4) + "_" + row.get(9), row.get(5), row.get(6), row.get(7), row.get(8), row.get(9));},
                RowEncoder.apply(joined_streams.schema())
        );

        joined_streams = joined_streams.withWatermark("timestampA", "50 milliseconds");

        Dataset<Row> windowedAvg = joined_streams.groupBy(
                functions.window(col("timestampA"), "15 seconds"),
                col("keyA"), col("idA")
        ).avg("valueA");

        windowedAvg = windowedAvg.withColumnRenamed("avg(valueA)", "value");
        windowedAvg = windowedAvg.withColumnRenamed("keyA", "key");
        windowedAvg = windowedAvg.withColumnRenamed("idA", "concatIds");

        windowedAvg = windowedAvg.selectExpr("CAST(key AS STRING)",
                "CAST(value AS STRING)", "CAST(concatIds AS STRING)");

        windowedAvg = windowedAvg.withColumn("value", concat(col("value"), col("concatIds")));

        windowedAvg
                .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
                .writeStream()
                .trigger(Trigger.ProcessingTime("0 milliseconds"))
                .format("kafka")
                .outputMode("append")
                .option("kafka.bootstrap.servers", IP)
                .option("topic", "iotOut")
                .option("checkpointLocation", "/home/ubuntu/iot_spark")
                .start();
        //joined_streams.writeStream()
        //        .outputMode("append")
        //        .format("console")
        //        .start().awaitTermination();
        spark.streams().awaitAnyTermination();
    }

    public static void twitter_topic_connection(String IP, SparkSession spark) throws Exception {
        System.out.println( "Initiating connection with twitter topic" );

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
        twitterA = twitterA.withColumn("idA", lit(""));

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
        twitterB = twitterB.withColumn("idB", lit(""));


        Dataset<Row> mapped_twitterA = twitterA.map((MapFunction<Row, Row>) row ->
                {return RowFactory.create(row.get(0), splitValue((String) row.get(1), 3),
                        row.get(2), row.get(3), get_id((String) row.get(1), 3));},
                RowEncoder.apply(twitterA.schema()));
        mapped_twitterA = mapped_twitterA.selectExpr("CAST(keyA AS STRING)", "CAST(valueA AS STRING)",
                "CAST(topicA AS STRING)", "CAST(timestampA AS TIMESTAMP)", "CAST(idA AS STRING)");
        Dataset<Row> mapped_twitterB = twitterB.map((MapFunction<Row, Row>) row ->
                {return RowFactory.create(row.get(0), splitValue((String) row.get(1), 3),
                        row.get(2), row.get(3), get_id((String) row.get(1), 3));},
                RowEncoder.apply(twitterB.schema()));
        mapped_twitterB = mapped_twitterB.selectExpr("CAST(keyB AS STRING)", "CAST(valueB AS STRING)",
                "CAST(topicB AS STRING)", "CAST(timestampB AS TIMESTAMP)", "CAST(idB AS STRING)");

        Dataset<Row> joined_streams = mapped_twitterA
                .join(mapped_twitterB, mapped_twitterA.col("keyA").equalTo(mapped_twitterB.col("keyB")));

        joined_streams = joined_streams .map(
                (MapFunction<Row, Row>) row ->
                {return RowFactory.create(row.get(0), twitter_counter((String)row.get(1) +"&-/-q&"+ (String)row.get(6)), row.get(2),
                        row.get(3), "!!432&%$(())#" + row.get(4) + "_" + row.get(9), row.get(5), row.get(6), row.get(7), row.get(8), row.get(9));},
                RowEncoder.apply(joined_streams.schema())
        );
        joined_streams = joined_streams.withWatermark("timestampA", "50 milliseconds");

        Dataset<Row> windowedAvg = joined_streams.groupBy(
                functions.window(col("timestampA"), "15 seconds"),
                col("keyA"), col("idA")
        ).df();

        windowedAvg = windowedAvg.withColumnRenamed("valueA", "value");
        windowedAvg = windowedAvg.withColumnRenamed("keyA", "key");
        windowedAvg = windowedAvg.withColumnRenamed("idA", "concatIds");

        windowedAvg = windowedAvg.selectExpr("CAST(key AS STRING)",
                "CAST(value AS STRING)", "CAST(concatIds AS STRING)");

        windowedAvg = windowedAvg.withColumn("value", concat(col("value"), col("concatIds")));

        windowedAvg
                .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
                .writeStream()
                .trigger(Trigger.ProcessingTime("0 milliseconds"))
                .format("kafka")
                .outputMode("append")
                .option("kafka.bootstrap.servers", IP)
                .option("topic", "twitterOut")
                .option("checkpointLocation", "/home/ubuntu/twitter_spark")
                .start();

        spark.streams().awaitAnyTermination();
    }

    public static void log_topic_connection(String IP, SparkSession spark) throws Exception {
        System.out.println( "Initiating connection with log topic" );
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
        logA = logA.withColumn("idA", lit(""));
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
        logB = logB.withColumn("idB", lit(""));

        Dataset<Row> mapped_logA = logA.map((MapFunction<Row, Row>) row ->
                {return RowFactory.create(row.get(0), splitValue((String) row.get(1), 2), row.get(2), row.get(3), get_id((String)row.get(1), 2));},
                RowEncoder.apply(logA.schema()));
        mapped_logA = mapped_logA.selectExpr("CAST(keyA AS STRING)", "CAST(valueA AS STRING)",
                "CAST(topicA AS STRING)", "CAST(timestampA AS TIMESTAMP)", "CAST(idA AS STRING)");
        Dataset<Row> mapped_logB = logB.map((MapFunction<Row, Row>) row ->
                {return RowFactory.create(row.get(0), splitValue((String) row.get(1), 2), row.get(2), row.get(3), get_id((String)row.get(1), 2));},
                RowEncoder.apply(logB.schema()));
        mapped_logB = mapped_logB.selectExpr("CAST(keyB AS STRING)", "CAST(valueB AS STRING)",
                "CAST(topicB AS STRING)", "CAST(timestampB AS TIMESTAMP)", "CAST(idB AS STRING)");

        Dataset<Row> joined_streams = mapped_logA
                .join(mapped_logB, mapped_logA.col("keyA").equalTo(mapped_logB.col("keyB")));

        joined_streams = joined_streams .map(
                (MapFunction<Row, Row>) row ->
                {return RowFactory.create(row.get(0), check_if_error((String)row.get(1) +" "+ (String)row.get(6)), row.get(2), row.get(3),
                        "!!432&%$(())#" + row.get(4) + "_" + row.get(9), row.get(5), row.get(6), row.get(7), row.get(8), row.get(9));},
                RowEncoder.apply(joined_streams.schema())
        );
        joined_streams = joined_streams.withWatermark("timestampA", "50 milliseconds");

        Dataset<Row> windowedAvg = joined_streams.groupBy(
                functions.window(col("timestampA"), "15 seconds"),
                col("keyA"), col("idA")
        ).df();
        windowedAvg = windowedAvg.withColumnRenamed("valueA", "value");
        windowedAvg = windowedAvg.withColumnRenamed("keyA", "key");
        windowedAvg = windowedAvg.withColumnRenamed("idA", "concatIds");

        windowedAvg = windowedAvg.selectExpr("CAST(key AS STRING)",
                "CAST(value AS STRING)", "CAST(concatIds AS STRING)");

        windowedAvg = windowedAvg.withColumn("value", concat(col("value"), col("concatIds")));

        windowedAvg
                .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
                .writeStream()
                .trigger(Trigger.ProcessingTime("0 milliseconds"))
                .format("kafka")
                .outputMode("append")
                .option("kafka.bootstrap.servers", IP)
                .option("topic", "logOut")
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
    //helper methods

    //method to parse records from the different sources
    public static String splitValue(String value, Integer source){ //topic A is 0, topic B is 1
        String[] parts = new String[0];
        String msg_value = "";
        if(source == 0){ //iot line separator
            //asumming an input of type 2707176363363894:2021-02-07 00:03:19,1612656199,63.3,17.4,ID
            //now would be 2707176363363894:2021-02-07 00:03:19,1612656199,63.3,17.4,ID
            parts = value.split(",");
            msg_value = parts[3];
        } else if (source == 2) { //logs line separator
            //[22/Jan/2019:03:56:16 +0330] "GET /image/60844/productModel/200x200 HTTP/1.1" 402 5667 "https://www.zanbil.ir/m/filter/b113" "Mozilla/5.0 (Linux; Android 6.0; ALE-L21 Build/HuaweiALE-L21) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.158 Mobile Safari/537.36" "-
            parts = value.split("(1.1\" )|(1.0\" )|(\" (?=\\d{3}))");
            parts = parts[1].split(" ");
            //System.out.println(Arrays.toString(parts));
            msg_value = parts[0];
        }else{ //twitter line separator
            msg_value = value.substring(0, 5);
        }
        return msg_value;
    }
//method to parse ids from messages
    public static String get_id(String value, Integer source){ //topic A is 0, topic B is 1
        String[] parts = new String[0];
        String msg_id = "";
        if(source == 0){ //iot line separator
            //asumming an input of type 2707176363363894:2021-02-07 00:03:19,1612656199,63.3,17.4,ID
            //now would be 2707176363363894:2021-02-07 00:03:19,1612656199,63.3,17.4,ID
            parts = value.split(",");
            msg_id = parts[4];
        } else if (source == 2) { //logs line separator
            //[22/Jan/2019:03:56:16 +0330] "GET /image/60844/productModel/200x200 HTTP/1.1" 402 5667 "https://www.zanbil.ir/m/filter/b113" "Mozilla/5.0 (Linux; Android 6.0; ALE-L21 Build/HuaweiALE-L21) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.158 Mobile Safari/537.36" "-
            msg_id = value.split("!!432&%\\$\\(\\(\\)\\)#")[1];
            //System.out.println(Arrays.toString(parts));
        }else{ //twitter line separator
            msg_id = value.split("!!432&%\\$\\(\\(\\)\\)#")[1];
            //System.out.println(Arrays.toString(parts));
        }

        return msg_id;
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
package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Properties;
import java.util.Scanner;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main ( String[] args ) throws IOException {
        //it has to read from the input topic (example: iotA) and from the output topic (out_iotA)
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "IP");
        properties.put("group.id", "test-topic-consumer");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        String path = "C:\\Users\\aevi1\\Documents\\memoria_testing\\latencia-iot-kafka-streams\\";
        new File(path).mkdirs();
        Scanner scanner = new Scanner(System.in);
        System.out.println("Elige el dataset del cual leer:");
        System.out.println("1.- Iniciar lectura de Twitter");
        System.out.println("2.- Iniciar lectura de Logs");
        System.out.println("3.- Iniciar lectura de IoT");
        int dataset = scanner.nextInt();
        if (dataset == 1){
            topic_reader(properties, "twitterA", "twitterOut"
                    , "twitterB", path);
        } else if (dataset == 2) {
            topic_reader(properties, "logA", "logOut",
                    "logB", path);
        }else {
            topic_reader(properties, "iotA", "iotOut",
                    "iotB", path);
        }
    }

    public static int records_count;
    public static int values_total_bytes;
    public static int keys_total_bytes;
    public static boolean timer_flag = true;
    public static Instant start;
    public static Instant end;
    public static void topic_reader(Properties properties, String input_topic_name, String output_topic_name,
                                    String input_topic_name2, String path) throws IOException {

        KafkaConsumer consumed_records = new KafkaConsumer(properties);
        consumed_records.subscribe(Arrays.asList(input_topic_name, input_topic_name2, output_topic_name));
        FileWriter input_file = new FileWriter(path+"\\input_timestamps.txt");
        input_file.write("topic;offset;key;value;timestamp\n");
        FileWriter output_file = new FileWriter(path+"\\output_timestamps.txt");
        output_file.write("topic;offset;key;value;timestamp\n");
        //thread to close the file
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            public void run() {
                end = Instant.now();
                long timeElapsed = Duration.between(start, end).toSeconds();
                try {
                    input_file.flush();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                try {
                    output_file.close();
                    calculate_throughput(timeElapsed);

                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }, "Shutdown-thread"));

        while(true){
            // lo que poll hace es esperar X tiempo a que lleguen nuevos datos antes de retornar un output vacío
            ConsumerRecords<String, String> records = consumed_records.poll(Duration.ofSeconds(10));
            for (ConsumerRecord<String, String> record : records){
                if(timer_flag){
                    start = Instant.now();
                    timer_flag = false;
                }
                String record_topic = record.topic();
                String line = String.format("%s;%s;%s;%s;%s%n", record.topic(), record.offset(), record.key(), record.value(), record.timestamp());
                if (record_topic.equals("iotA") || record_topic.equals("iotB") ){ //|| record_topic.equals("logA") || record_topic.equals("twitterA") || record_topic.equals("logB") || record_topic.equals("twitterB")
                    input_file.write(line);
                }else{
                    //OUTPUT TOPIC EVENT CONSUMED
                    records_count += 1;
                    values_total_bytes += record.serializedValueSize();
                    keys_total_bytes += record.serializedKeySize();
                    output_file.write(line);
                }
            }
        }
    }

    public static void calculate_throughput(long time_elapsed_in_secs){
        //TODO: Calcular time_elapsed_in_secs
        double total_megabytes = (keys_total_bytes+values_total_bytes)/1000000;
        double throughput = total_megabytes/time_elapsed_in_secs;
        double total_bytes = keys_total_bytes+values_total_bytes;
        System.out.println("Received a total of "+records_count+" records in "+time_elapsed_in_secs+" seconds");
        System.out.println("Total bytes received: "+total_bytes);
        System.out.println("Throughput is equal to "+throughput+" MB/s.");

    }

}

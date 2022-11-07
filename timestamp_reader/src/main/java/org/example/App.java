package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
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
        properties.put("bootstrap.servers", "34.176.127.40:9092");
        properties.put("group.id", "test-topic-consumer");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        Scanner scanner = new Scanner(System.in);
        System.out.println("Elige el dataset del cual leer:");
        System.out.println("1.- Iniciar lectura de Twitter");
        System.out.println("2.- Iniciar lectura de Logs");
        System.out.println("3.- Iniciar lectura de IoT");
        System.out.println("4.- Calcular latencias(?");
        int dataset = scanner.nextInt();
        if (dataset == 1){
            topic_reader(properties, "twitterA", "output_twitterA"
                    , "twitterB", "output_twitterB");
        } else if (dataset == 2) {
            topic_reader(properties, "logA", "output_logA",
                    "logB", "logB");
        }else {
            topic_reader(properties, "iotA", "output_iotA",
                    "iotB", "output_iotB");
        }
    }

    public static void topic_reader(Properties properties, String input_topic_name, String output_topic_name,
                                    String input_topic_name2, String output_topic_name2) throws IOException {

        //dont forget to test this consumer with framework consumer at the same time
        KafkaConsumer consumed_records = new KafkaConsumer(properties);
        consumed_records.subscribe(Arrays.asList(input_topic_name, input_topic_name2, output_topic_name, output_topic_name2));
        FileWriter input_file = new FileWriter("C:\\Users\\aevi1\\OneDrive\\Documentos\\timestamps\\input_timestamps.txt");
        input_file.write("topic, offset, key, value, timestamp\n");
        FileWriter output_file = new FileWriter("C:\\Users\\aevi1\\OneDrive\\Documentos\\timestamps\\output_timestamps.txt");
        output_file.write("topic, offset, key, value, timestamp\n");
        //thread to close the file
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            public void run() {
                try {
                    input_file.flush();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                try {
                    output_file.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }, "Shutdown-thread"));

        while(true){
            ConsumerRecords<String, String> records = consumed_records.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records){
                String record_topic = record.topic();
                String line = String.format("%s, %s, %s, %s, %s%n", record.topic(), record.offset(), record.key(), record.value(), record.timestamp());
                if (record_topic.equals("iotA") || record_topic.equals("logA") || record_topic.equals("twitterA") ||
                        record_topic.equals("iotB") || record_topic.equals("logB") || record_topic.equals("twitterB")){
                    input_file.write(line);
                    System.out.println("kafka record written to input file");
                }else{
                    output_file.write(line);
                    System.out.println("kafka record written to output file");
                }
            }
        }
    }

}

package twitterclass;

import com.google.gson.reflect.TypeToken;
import com.twitter.clientlib.JSON;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.*;
import java.lang.reflect.Type;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.HashSet;
import java.util.Set;
import java.io.InputStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.twitter.clientlib.TwitterCredentialsBearer;
import com.twitter.clientlib.ApiException;
import com.twitter.clientlib.api.TwitterApi;
import com.twitter.clientlib.model.*;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.json.JSONException;
import org.json.JSONObject;

public class TestProducer {
    public TestProducer(int latency, int dataset) throws IOException {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "xx.xxx.xxx.xxx:9092");
        //
        //properties.put("linger.ms", 1000); //it will wait 1 second at most before sending a request
        //properties.put("batch.size", 1000000); // will sent a request when messages sum this quantity
        properties.put("client.id", "datasetsProducer");

        //
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // thread that runs when the program is closed
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                end = Instant.now();
                long timeElapsed = Duration.between(start, end).toSeconds();

                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                //calculating MB sent per second
                long total_bytes_sent_A = total_values_bytes_typeA + total_keys_bytes_typeA;
                long total_bytes_sent_B = total_values_bytes_typeB + total_keys_bytes_typeB;
                double total_mb_A = total_bytes_sent_A/1000000;
                double total_mb_B = total_bytes_sent_B/1000000;
                double mb_per_second_A = total_mb_A/timeElapsed;
                double mb_per_second_B = total_mb_B/timeElapsed;

                //
                System.out.println("-- TOTAL TIME ELAPSED FROM FIRST MSG SENT: "+ timeElapsed +" seconds --");
                System.out.println("TOPIC A INFO");
                System.out.println("Records sent successfully: " + records_count_typeA);
                System.out.println("Total values bytes sent: " + total_values_bytes_typeA);
                System.out.println("Total keys bytes sent: " + total_keys_bytes_typeA);
                System.out.println("Total megabytes per second: " + mb_per_second_A+" MB/s");
                System.out.println("------------------------------------------------------");
                System.out.println("TOPIC B INFO");
                System.out.println("Records sent successfully: " + records_count_typeB);
                System.out.println("Total values bytes sent: " + total_values_bytes_typeB);
                System.out.println("Total keys bytes sent: " + total_keys_bytes_typeB);
                System.out.println("Total megabytes per second: " + mb_per_second_B+" MB/s");

            }
        });
        //
        if (dataset == 1){
            while(true){
                twitter_producer(properties);
            }
        } else if (dataset == 2) {
            while(true){
                logs_producer(properties);
            }
        }else{
            while(true){
                iot_producer(properties);
            }
        }

    }
    public void twitter_producer(Properties properties){
        TwitterApi apiInstance = new TwitterApi(new TwitterCredentialsBearer(
                "AAAAAAAAAAAAAAAAAAAAAEcAdQEAAAAA8%2BGwVF4CJFpQk8zovoLZDOluGr8%3DgToycyJcSbW07qOyMUo3XN0328AHuf0OkWucxAB8NWw0NBIsXY"));

        Set<String> tweetFields = new HashSet<>();
        tweetFields.add("author_id");
        tweetFields.add("id");
        tweetFields.add("created_at");
        try {
            InputStream streamResult = apiInstance.tweets().sampleStream()
                    .backfillMinutes(0)
                    .tweetFields(tweetFields)
                    .execute();


// An example of how to use the streaming directly using the InputStream result (Without TweetsStreamListenersExecutor)
      try{
         JSON json = new JSON();
         Type localVarReturnType = new TypeToken<StreamingTweetResponse>(){}.getType();
         BufferedReader reader = new BufferedReader(new InputStreamReader(streamResult));
         String line = reader.readLine();
         while (line != null) {
           if(line.isEmpty()) {
             System.err.println("==> " + line.isEmpty());
             line = reader.readLine();
             continue;
            }
           try { //here the record is sent to kafka
               JSONObject jsonObject = new JSONObject(line);
               String tweetText = jsonObject.getJSONObject("data").getString("text");
               //String id = jsonObject.getJSONObject("data").getString("author_id");
               //System.out.println(tweetText);
               String key = "1";
               String value = tweetText + "!!432&%$(())#" + generate_uniq_id();

               KafkaProducer producer = new KafkaProducer(properties);
               int j = 0;
               int i = 0;
               while(tweetText != null){ //&& j != 20
                   if(i == 0) {
                       send_record_to_kafka(producer, key, value, "twitterA", 0);
                       i = 1;
                   }else{
                       send_record_to_kafka(producer, key, value, "twitterB", 1);
                       i = 0;
                   }
                   j += 1;
               }

               producer.close();
           }catch (JSONException err){
               System.out.println("Error al convertir el string en JSON");
               System.out.println(err);
           }
           //Object jsonObject = json.getGson().fromJson(line, localVarReturnType);
           //System.out.println(jsonObject != null ? jsonObject.toString() : "Null object");
           line = reader.readLine();
         }
      }catch (Exception e) {
        e.printStackTrace();
        System.out.println(e);
      }
        } catch (ApiException e) {
            System.err.println("Status code: " + e.getCode());
            System.err.println("Reason: " + e.getResponseBody());
            System.err.println("Response headers: " + e.getResponseHeaders());
            e.printStackTrace();
        }
    }

    public void logs_producer(Properties properties) throws IOException {
        KafkaProducer producer = new KafkaProducer(properties);
        String path = "C:/Users/aevi1/OneDrive/Documentos/access.log";
        String line = "";
        BufferedReader br = new BufferedReader(new FileReader(path));
        int j = 0;
        int i = 0;
        while((line = br.readLine()) != null ){ //&& j != 10
            String[] record_values = split_lines(line, 1);
            //parts[0] + ":" + parts[1];
            String key = record_values[0];
            String value = record_values[1] + "!!432&%$(())#" + generate_uniq_id();
            if(i == 0) {
                send_record_to_kafka(producer, key, value, "logA", 0);
                i = 1;
            }else{
                send_record_to_kafka(producer, key, value, "logB", 1);
                i = 0;
            }
            j += 1;
        }

        producer.close();
    }
    public void iot_producer(Properties properties) throws IOException {
        KafkaProducer producer = new KafkaProducer(properties);
        String path = "C:\\Users\\aevi1\\Documents\\MEMORIA\\codigos\\datasets\\IoT\\temperature.csv";
        String line;
        BufferedReader br = new BufferedReader(new FileReader(path));
        int j = 0;
        int i = 0;
        while((line = br.readLine()) != null){ //&& j != 20
            //parts[4] + ":" + parts[0] + "," + parts[1] + "," + parts[2] + "," + parts[3];
            String[] record_values = split_lines(line, 0);
            String key = record_values[4];
            String value = record_values[0] + "," + record_values[1] + "," + record_values[2] + "," + record_values[3]
                    + "," + generate_uniq_id();
            //System.out.println(key);
            //System.out.println(value);
            if(i == 0) {
                send_record_to_kafka(producer, key, value, "iotA", 0);
                i = 1;
            }else{
                send_record_to_kafka(producer, key, value, "iotB", 1);
                i = 0;
            }
            j += 1;
        }
        System.out.println("EOF - Starting new iteration");
        producer.close();
    }
    //method to calculate metrics about the sent data
    public static int records_count_typeA;
    public static int records_count_typeB;
    public static int total_values_bytes_typeA;
    public static int total_values_bytes_typeB;
    public static int total_keys_bytes_typeA;
    public static int total_keys_bytes_typeB;
    public static boolean timer_flag = true;
    public static Instant start;
    public static Instant end;
    public static Instant current_time;
    public void send_record_to_kafka(KafkaProducer producer, String key, String value, String topic_name, Integer topic_type){
        ProducerRecord record = new ProducerRecord(topic_name, key, value);
        if(topic_type == 0) { //topic type A
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    if(e != null)
                        e.printStackTrace();
                    records_count_typeA += 1;
                    total_values_bytes_typeA += metadata.serializedValueSize();
                    total_keys_bytes_typeA += metadata.serializedKeySize();
                    if(timer_flag){
                        start = Instant.now();
                        timer_flag = false;
                    }
                    // for debugging reasons
                    current_time = Instant.now();
                    long timeElapsed = Duration.between(start, current_time).toSeconds();
                    if(timeElapsed == 10){
                        System.exit(0);
                    }
                    //
                }
            });
        }else{ //topic type B
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    if(e != null)
                        e.printStackTrace();
                    records_count_typeB += 1;
                    total_values_bytes_typeB += metadata.serializedValueSize();
                    total_keys_bytes_typeB += metadata.serializedKeySize();
                }
            });
        }
    }
    public static long unique_id = 0;
    public String generate_uniq_id(){
        unique_id += 1;
        return String.valueOf(unique_id);
    }
    public String[] split_lines(String line, Integer source){
        String[] parts = new String[0];
        if(source == 0){ //iot line separator
            //line type
            //2021-02-07 00:03:19,1612656199,63.3,17.4,2707176363363894
            parts = line.split(",");
            //parts[4] + ":" + parts[0] + "," + parts[1] + "," + parts[2] + "," + parts[3];

        } else if (source == 1) { //logs line separador
            // line type
            // 31.56.96.51 - - [22/Jan/2019:03:56:16 +0330] "GET /image/60844/productModel/200x200 HTTP/1.1" 200 5667 "https://www.zanbil.ir/m/filter/b113" "Mozilla/5.0 (Linux; Android 6.0; ALE-L21 Build/HuaweiALE-L21) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.158 Mobile Safari/537.36" "-"
            parts = line.split("( - - )|( - )");
            //parts[0] + ":" + parts[1];
        }
        return parts;
    }
}

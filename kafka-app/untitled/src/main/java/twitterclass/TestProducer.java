package twitterclass;

import com.google.gson.reflect.TypeToken;
import com.twitter.clientlib.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.*;
import java.lang.reflect.Type;
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
import org.json.JSONException;
import org.json.JSONObject;

public class TestProducer {
    public TestProducer(int latency, int dataset) throws IOException {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "34.176.255.191:9092");
        properties.put("linger.ms", latency);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        if (dataset == 1){
            twitter_producer(properties);
        } else if (dataset == 2) {
            logs_producer(properties);
        }else{
            iot_producer(properties);
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
               String value = tweetText;

               KafkaProducer producer = new KafkaProducer(properties);
               int j = 0;
               int i = 0;
               while(tweetText != null && j != 10){
                   if(i == 0) {
                       producer.send(new ProducerRecord("twitterA", key, value));
                       i = 1;
                   }else{
                       producer.send(new ProducerRecord("twitterB", key, value));
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
        while((line = br.readLine()) != null && j != 10){
            String[] record_values = split_lines(line, 1);
            //parts[0] + ":" + parts[1];
            String key = record_values[0];
            String value = record_values[1];
            if(i == 0) {
                producer.send(new ProducerRecord("logsA", key, value));
                i = 1;
            }else{
                producer.send(new ProducerRecord("logsB", key, value));
                i = 0;
            }
            j += 1;
        }

        producer.close();
    }
    public void iot_producer(Properties properties) throws IOException {
        KafkaProducer producer = new KafkaProducer(properties);
        String path = "C:/Users/aevi1/OneDrive/Documentos/temperature.csv";
        String line;
        BufferedReader br = new BufferedReader(new FileReader(path));
        int j = 0;
        int i = 0;
        while((line = br.readLine()) != null && j != 2){
            //parts[4] + ":" + parts[0] + "," + parts[1] + "," + parts[2] + "," + parts[3];
            String[] record_values = split_lines(line, 0);
            String key = record_values[4];
            String value = record_values[0] + "," + record_values[1] + "," + record_values[2] + "," + record_values[3];
            System.out.println(key);
            System.out.println(value);
            if(i == 0) {
                producer.send(new ProducerRecord("iotA", key, value));
                i = 1;
            }else{
                producer.send(new ProducerRecord("iotB", key, value));
                i = 0;
            }
            j += 1;
        }

        producer.close();
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

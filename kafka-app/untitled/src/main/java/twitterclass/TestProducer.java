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

import com.twitter.clientlib.TwitterCredentialsBearer;
import com.twitter.clientlib.ApiException;
import com.twitter.clientlib.api.TwitterApi;
import com.twitter.clientlib.model.*;
import org.json.JSONException;
import org.json.JSONObject;

@SuppressWarnings("ALL")
public class TestProducer {
    public TestProducer(int latency, int dataset) throws IOException {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "34.176.50.2:9092");
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
           try {
               JSONObject jsonObject = new JSONObject(line);
               String tweetText = jsonObject.getJSONObject("data").getString("text");

               KafkaProducer producer = new KafkaProducer(properties);
               int i = 0;
               while(tweetText != null && i != 10){
                   producer.send(new ProducerRecord("test", tweetText));
                   i += 1;
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
        String path = "/Users/aevi1/Documents/2022-1/MEMORIA/codigos/datasets/weblogs/access.log";
        String line = "";
        BufferedReader br = new BufferedReader(new FileReader(path));
        int i = 0;
        while((line = br.readLine()) != null && i != 10){
            //System.out.println(line);
            producer.send(new ProducerRecord("test", line));
            i += 1;
        }

        producer.close();
    }
    public void iot_producer(Properties properties) throws IOException {
        KafkaProducer producer = new KafkaProducer(properties);
        String path = "/Users/aevi1/Documents/2022-1/MEMORIA/codigos/datasets/IoT/temperature.csv";
        String line;
        BufferedReader br = new BufferedReader(new FileReader(path));
        int i = 0;
        while((line = br.readLine()) != null && i != 10){
            //System.out.println(line);
            producer.send(new ProducerRecord("test", line));
            i += 1;
        }

        producer.close();
    }
}

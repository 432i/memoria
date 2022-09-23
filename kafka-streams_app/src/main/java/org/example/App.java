package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.*;

import java.util.Properties;

/**
 * Hello world!
 *
 */
public class App
{
    public static void main( String[] args ) throws Exception{
        System.out.println( "Iniciando consumidor de kafka streams" );
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "34.176.255.191:9092");
        final StreamsBuilder builder = new StreamsBuilder();

        final Serde<String> stringSerde = Serdes.String();

        KStream<String, String> source = builder.stream("test3",
                Consumed.with(stringSerde, stringSerde).withTimestampExtractor(new StreamsTimestampExtractor.myTimestampExtractor()) );
        source.foreach((key, value) -> System.out.println("valor: " + value+", llave: " + key));
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

    }
}



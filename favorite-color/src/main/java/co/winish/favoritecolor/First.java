package co.winish.favoritecolor;

import co.winish.config.KafkaStreamsConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;


public class First {

    public static void main(String[] args) {

        StreamsBuilder inputBuilder = new StreamsBuilder();
        KStream<String, String> inputStream = inputBuilder.stream("fav-color-input");



        KTable<String, Long> colorCounts = inputStream
                .toTable()
                .groupBy((name, color) -> new KeyValue<String, String>(color, color))
                .count(Named.as("Counts"));

        colorCounts.toStream().to("fav-color-output", Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(inputBuilder.build(), KafkaStreamsConfig.getSimpleProperties("fav-color-1"));
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}

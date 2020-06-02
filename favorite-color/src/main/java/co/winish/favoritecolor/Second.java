package co.winish.favoritecolor;

import co.winish.config.KafkaStreamsConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;

public class Second {

    public static void main(String[] args) {

        StreamsBuilder inputBuilder = new StreamsBuilder();
        KStream<String, String> inputStream = inputBuilder.stream("fav-color-input-2");

        inputStream.filter((absent, message) -> message.contains(","))
                .map((key, message) -> new KeyValue<String, String>(message.split(",")[0], message.split(",")[1]))
                .filter((user, color) -> Arrays.asList("red", "black", "white").contains(color))
                .to("intermediary-topic");

        KTable<String, String> colorCounts = inputBuilder.table("intermediary-topic");

        colorCounts.groupBy((user, color) -> new KeyValue<String, String>(color, color))
                .count(Named.as("Counts"))
                .toStream()
                .to("fav-color-output-2", Produced.with(Serdes.String(), Serdes.Long()));


        KafkaStreams streams = new KafkaStreams(inputBuilder.build(), KafkaStreamsConfig.getSimpleProperties("fav-color-2"));
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

}

package co.winish.wordcount;

import co.winish.config.KafkaStreamsConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;


import java.util.Arrays;

public class Main {

    public static void main(String[] args) {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> inputStream = builder.stream("word-count-input");

        KTable<String, Long> wordCounts = inputStream
                .mapValues(value -> value.toLowerCase())
                .flatMapValues(textLine -> Arrays.asList(textLine.split("\\W+")))
                .selectKey((key, value) -> value)
                .groupByKey()
                .count(Named.as("Counts"));

        wordCounts.toStream().to("word-count-output", Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(), KafkaStreamsConfig.getSimpleProperties("word-counter"));
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}

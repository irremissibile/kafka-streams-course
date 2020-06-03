package co.winish.bankbalance;

import co.winish.config.KafkaStreamsConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Instant;

public class Main {

    public static void main(String[] args) {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, JsonNode> bankTransactions = builder.stream("transactions",
                Consumed.with(Serdes.String(), KafkaStreamsConfig.getJsonSerde()));


        ObjectNode initialBalance = JsonNodeFactory.instance.objectNode();
        initialBalance.put("count", 0);
        initialBalance.put("balance", 0);
        initialBalance.put("time", Instant.ofEpochMilli(0L).toString());


        KTable<String, JsonNode> bankBalance = bankTransactions
                .groupByKey()
                //.groupByKey(Serialized.with(Serdes.String(), KafkaStreamsConfig.getJsonSerde()))
                .aggregate(
                        () -> initialBalance,
                        (key, transaction, balance) -> newBalance(transaction, balance),
                        Materialized.<String, JsonNode, KeyValueStore<Bytes, byte[]>>as("bank-balance-agg")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(KafkaStreamsConfig.getJsonSerde())
                );

        bankBalance.toStream().to("bank-balance", Produced.with(Serdes.String(), KafkaStreamsConfig.getJsonSerde()));

        KafkaStreams streams = new KafkaStreams(builder.build(), KafkaStreamsConfig.getExactlyOnceProperties("bank-balance-app"));
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static JsonNode newBalance(JsonNode transaction, JsonNode balance) {
        ObjectNode newBalance = JsonNodeFactory.instance.objectNode();
        newBalance.put("count", balance.get("count").asInt() + 1);
        newBalance.put("balance", balance.get("balance").asInt() + transaction.get("amount").asInt());
        newBalance.put("time", transaction.get("time").asText());

        return newBalance;
    }

}

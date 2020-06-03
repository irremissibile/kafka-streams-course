package co.winish.streamenricher;

import co.winish.config.KafkaStreamsConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(Main.class);

        StreamsBuilder builder = new StreamsBuilder();

        GlobalKTable<String, String> usersGlobalTable = builder.globalTable("user-table");
        KStream<String, String> userPurchases = builder.stream("user-purchases");

        KStream<String, String> userPurchasesEnrichedJoin =
                userPurchases.join(usersGlobalTable,
                        (key, value) -> key,
                        (userPurchase, userInfo) -> "Purchase = " + userPurchase + ", UserInfo = [" + userInfo + "]"
                );

        userPurchasesEnrichedJoin.to("user-purchases-enriched-inner-join");

        KStream<String, String> userPurchasesEnrichedLeftJoin =
                userPurchases.leftJoin(usersGlobalTable,
                        (key, value) -> key,
                        (userPurchase, userInfo) -> {
                            if (userInfo != null)
                                return "Purchase=" + userPurchase + ",UserInfo=[" + userInfo + "]";
                            else
                                return "Purchase=" + userPurchase + ",UserInfo=null";
                        }
                );

        userPurchasesEnrichedLeftJoin.to("user-purchases-enriched-left-join");


        KafkaStreams streams = new KafkaStreams(builder.build(), KafkaStreamsConfig.getSimpleProperties("enricher"));
        streams.start();

        streams.localThreadsMetadata().forEach(data -> logger.info("data: " + data));

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

}

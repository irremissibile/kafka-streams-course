package co.winish.bankbalance;

import co.winish.config.KafkaStreamsConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;

public class TransactionsProducer {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(TransactionsProducer.class);

        Producer<String, JsonNode> producer = new KafkaProducer<String, JsonNode>(KafkaStreamsConfig.getIdempotentJsonProducerProperties());

        int i = 0;
        while (true) {
            logger.info("Producing batch: " + i);
            try {
                producer.send(newRandomTransaction("Dzhamil"));
                Thread.sleep(100);
                producer.send(newRandomTransaction("Jean Renault"));
                Thread.sleep(100);
                producer.send(newRandomTransaction("Emma"));
                Thread.sleep(100);

                i++;
            } catch (InterruptedException e) {
                break;
            }
        }
        producer.close();
    }

    public static ProducerRecord<String, JsonNode> newRandomTransaction(String username) {
        ObjectNode transaction = JsonNodeFactory.instance.objectNode();

        transaction.put("name", username);
        transaction.put("amount", ThreadLocalRandom.current().nextInt(0, 101));
        transaction.put("time", Instant.now().toString());
        return new ProducerRecord<String, JsonNode>("transactions", username, transaction);
    }
}

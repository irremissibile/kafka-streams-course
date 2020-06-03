package co.winish.streamenricher;

import co.winish.config.KafkaStreamsConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

public class InfoProducer {

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        Logger logger = LoggerFactory.getLogger(InfoProducer.class);

        Producer<String, String> producer = new KafkaProducer<>(KafkaStreamsConfig.getIdempotentStringProducerProperties());

        logger.info("\nExample 1 - new user\n");
        producer.send(userRecord("john", "First=John,Last=Doe,Email=john.doe@gmail.com")).get();
        producer.send(purchaseRecord("john", "Apples and Bananas (1)")).get();
        Thread.sleep(10000);

        logger.info("\nExample 2 - non existing user\n");
        producer.send(purchaseRecord("bob", "Kafka Udemy Course (2)")).get();
        Thread.sleep(10000);

        logger.info("\nExample 3 - update to user\n");
        producer.send(userRecord("john", "First=Johnny,Last=Doe,Email=johnny.doe@gmail.com")).get();
        producer.send(purchaseRecord("john", "Oranges (3)")).get();
        Thread.sleep(10000);

        logger.info("\nExample 4 - non existing user then user\n");
        producer.send(purchaseRecord("stephane", "Computer (4)")).get();
        producer.send(userRecord("stephane", "First=Stephane,Last=Maarek,GitHub=simplesteph")).get();
        producer.send(purchaseRecord("stephane", "Books (4)")).get();
        producer.send(userRecord("stephane", null)).get();
        Thread.sleep(10000);

        logger.info("\nExample 5 - user then delete then data\n");
        producer.send(userRecord("alice", "First=Alice")).get();
        producer.send(userRecord("alice", null)).get();
        producer.send(purchaseRecord("alice", "Apache Kafka Series (5)")).get();
        Thread.sleep(10000);

        System.out.println("End of demo");
        producer.close();
    }


    private static ProducerRecord<String, String> userRecord(String key, String value){
        return new ProducerRecord<>("user-table", key, value);
    }


    private static ProducerRecord<String, String> purchaseRecord(String key, String value){
        return new ProducerRecord<>("user-purchases", key, value);
    }
}

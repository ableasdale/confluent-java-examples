package io.confluent.csg.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class TLSConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "<hostname>:9093");
        props.put("security.protocol", "SSL");
        props.put("ssl.truststore.location", "src/main/resources/kafka.client.truststore.jks"); // .jks format
        props.put("ssl.truststore.password","confluent");
        props.put("ssl.keystore.location", "src/main/resources/kafka.client.keystore.jks"); // .jks format
        props.put("ssl.keystore.password", "confluent");

        //props.put("group.id", "group-1111");
        props.put("enable.auto.commit", "false");
        //props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "earliest");
        //props.put("session.timeout.ms", "30000");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-java-getting-started");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer kafkaConsumer = new KafkaConsumer<>(props);
        kafkaConsumer.subscribe(Arrays.asList("acl-test"));
        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(5));
            for (ConsumerRecord<String, String> record : records)
                LOG.info("Partition: " + record.partition() + " Offset: " + record.offset() + " Value: " + record.value() + " ThreadID: " + Thread.currentThread().getId());

        }
    }
}

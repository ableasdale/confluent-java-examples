package io.confluent.cse.kafkaclient.consumer;

import io.confluent.cse.kafkaclient.Config;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class TLSConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", Config.KAFKA_TLS_BOOTSTRAP_SERVER);

        // Consumer TLS Configuration
        props.put("security.protocol", "SSL");
        props.put("ssl.truststore.location", "src/main/resources/kafka.client.truststore.jks");
        props.put("ssl.truststore.password", "confluent");
        props.put("ssl.keystore.location", "src/main/resources/kafka.client.keystore.jks");
        props.put("ssl.keystore.password", "confluent");

        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, Config.GROUP_ID);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        try (var consumer = new KafkaConsumer<String, String>(props)) {
            consumer.subscribe(List.of(Config.TOPIC));
            while (true) {
                ConsumerRecords<?, ?> records = consumer.poll(Duration.ofSeconds(5));
                for (ConsumerRecord<?, ?> record : records)
                    LOG.info(String.format("Partition: %s Offset: %s Value: %s Thread Id: %s",record.partition(),record.offset(),record.value(),Thread.currentThread().getId()));
            }
        }
    }
}

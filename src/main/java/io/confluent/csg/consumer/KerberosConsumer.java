package io.confluent.csg.consumer;

import org.apache.kafka.clients.consumer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class KerberosConsumer {
    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static void main(String[] args) {

        // Kerberos
        System.setProperty("sun.security.krb5.debug", "true");
        System.setProperty("sun.security.spnego.debug", "true");
        System.setProperty("java.security.auth.login.config", "src/main/resources/kafka_client_jaas.conf");
        System.setProperty("java.security.krb5.conf", "src/main/resources/krb5.conf");

        final String topic = "kafka-security-topic";

        Properties props = new Properties();
        props.put("bootstrap.servers", "<hostname>:9094");
        props.put("enable.auto.commit", "false");

        props.put("auto.offset.reset", "earliest");

        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.kerberos.service.name", "kafka");
        props.put("sasl.mechanism", "GSSAPI");
        props.put("ssl.truststore.location", "src/main/resources/kafka.client.truststore.jks");
        props.put("ssl.truststore.password", "confluent");
        props.put("ssl.keystore.location", "src/main/resources/kafka.client.keystore.jks");
        props.put("ssl.keystore.password", "confluent");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-java-getting-started-gg");

        final Consumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    long offset = record.offset();
                    String key = record.key();
                    String value = record.value();
                    LOG.info(
                            String.format("Consumed event from topic %s at offset %d: key = %-5s value = %s", topic, offset, key, value));
                }
            }
        } finally {
            consumer.close();
        }
    }
}
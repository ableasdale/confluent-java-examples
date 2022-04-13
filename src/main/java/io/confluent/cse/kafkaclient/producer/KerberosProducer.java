package io.confluent.cse.kafkaclient.producer;

import io.confluent.cse.kafkaclient.Config;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Properties;
import java.util.Random;

public class KerberosProducer {

    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static void main(String[] args) {

        // Kerberos Debugging Options (set root log level to debug in line 8 of src/main/resources/logback.xml for further diagnostic information)
        System.setProperty("sun.security.krb5.debug", "true");
        System.setProperty("sun.security.spnego.debug", "true");
        System.setProperty("java.security.auth.login.config", "src/main/resources/kafka_client_jaas.conf");
        System.setProperty("java.security.krb5.conf", "src/main/resources/krb5.conf");

        // Load producer configuration settings from a local file
        final Properties props = new Properties();
        props.put("bootstrap.servers", Config.KAFKA_TLS_SASL_BOOTSTRAP_SERVER);

        // Kerberos Producer Configuration
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.kerberos.service.name", "kafka");
        props.put("sasl.mechanism", "GSSAPI");

        // This is the necessary configuration for configuring TLS/SSL on the Producer
        props.put("ssl.truststore.location", "src/main/resources/kafka.client.truststore.jks");
        props.put("ssl.truststore.password", "confluent");
        props.put("ssl.keystore.location", "src/main/resources/kafka.client.keystore.jks");
        props.put("ssl.keystore.password", "confluent");

        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        final String topic = Config.TOPIC;

        String[] users = {"eabara", "jsmith", "sgarcia", "jbernard", "htanaka", "awalther"};
        String[] items = {"book", "alarm clock", "t-shirts", "gift card", "batteries"};
        Producer<String, String> producer = new KafkaProducer<>(props);

        final Long numMessages = 1000L;

        for (Long i = 0L; i < numMessages; i++) {
            Random rnd = new Random();
            String user = users[rnd.nextInt(users.length)];
            String item = items[rnd.nextInt(items.length)];

            producer.send(
                    new ProducerRecord<>(topic, user, item),
                    (event, ex) -> {
                        if (ex != null)
                            ex.printStackTrace();
                        else
                            LOG.info(String.format("Produced event to topic %s: key = %-10s value = %s", topic, user, item));
                    });
        }

        producer.flush();
        LOG.info(String.format("%s events were produced to topic %s%n", numMessages, topic));
        producer.close();
    }
}

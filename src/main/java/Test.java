
import org.apache.kafka.clients.consumer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Test {
    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    public static void main(String[] args) {
        LOG.info("started");
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
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-java-getting-started-nnn");
        String topic = "acl-test";
        final Consumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    String key = record.key();
                    String value = record.value();
                    LOG.info(
                            String.format("Consumed event from topic %s: key = %-10s value = %s", topic, key, value));
                }
            }
        } finally {
            consumer.close();
        }

    }
}

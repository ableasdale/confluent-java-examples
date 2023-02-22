package io.confluent.cse.kafkaclient.consumer;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import io.confluent.cse.kafkaclient.Config;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;

public class ConsumerHeaders {

    // docker-compose exec broker1 kafka-console-producer --bootstrap-server broker1:9091 --topic demo-perf-topic
    // docker-compose exec broker1 kafka-console-consumer --bootstrap-server broker1:9091 --from-beginning --topic demo-perf-topic --property print.timestamp=true
    // docker-compose exec broker1 kafka-run-class kafka.tools.GetOffsetShell --broker-list broker1:9091 --topic demo-perf-topic --command-config /tmp/consumer.properties

    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", Config.KAFKA_DOCKER_INSTANCE);

        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");
        // deliberately slow down the Consumer for testing
        props.put("max.poll.records", "1");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, Config.GROUP_ID);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        try (var consumer = new KafkaConsumer<String, String>(props)) {
            consumer.subscribe(List.of(Config.TOPIC));
            while (true) {
                ConsumerRecords<?, ?> records = consumer.poll(Duration.ofSeconds(5));
                for (ConsumerRecord<?, ?> record : records) {
                    LOG.info("*** HEADERS ***");
                    for (Header s : record.headers()) {
                        LOG.info(s.key() + " : " + s.value());
                    }
                    LOG.info("Broker (Unix) Timestamp in ms: %d".formatted(record.timestamp()));
                    Date currentTime = new Date(record.timestamp());
                    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS", Locale.ENGLISH);
                    LOG.info(formatter.format(currentTime));
                    LOG.info("Leader Epoch: "+record.leaderEpoch());
                    LOG.info(String.format("Partition: %s Offset: %s Value: %s Thread Id: %s", record.partition(), record.offset(), record.value(), Thread.currentThread().getId()));

                    LOG.info("*** OFFSET INFO ***");
                    // Get the diff of current position and latest offset
                    Set<TopicPartition> partitions = new HashSet<>();
                    TopicPartition actualTopicPartition = new TopicPartition(record.topic(), record.partition());
                    partitions.add(actualTopicPartition);
                    Long actualEndOffset = consumer.endOffsets(partitions).get(actualTopicPartition);
                    long actualPosition = consumer.position(actualTopicPartition);
                    LOG.info(String.format("Consumer Lag difference: %s   (actualEndOffset:%s; actualPosition=%s)", actualEndOffset-actualPosition ,actualEndOffset, actualPosition));


                }
            }
        }

    }
}

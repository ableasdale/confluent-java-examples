package io.confluent.cse.kafkaclient;

public class Config {
    public static final String KAFKA_HOST = "ubuntu";
    public static final int TLS_PORT = 9093;
    public static final int TLS_SASL_PORT = 9094;
    public static final String KAFKA_DOCKER_INSTANCE = "localhost:29091";
    public static final String KAFKA_TLS_BOOTSTRAP_SERVER = "%s:%d".formatted(KAFKA_HOST, TLS_PORT);
    public static final String KAFKA_TLS_SASL_BOOTSTRAP_SERVER = "%s:%d".formatted(KAFKA_HOST, TLS_SASL_PORT);
    public static final String[] TOPIC = new String[]{"example-topic", "demo-perf-topic"};
    public static final String GROUP_ID = "my-consumer-groupname";
}

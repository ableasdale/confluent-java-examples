package io.confluent.cse;

public class Config {

    public static final String KAFKA_HOST = "ec2-3-11-122-162.eu-west-2.compute.amazonaws.com";
    public static final int TLS_PORT = 9093;
    public static final int SASL_PORT = 9094;

    public static final String KAFKA_TLS_BOOTSTRAP_SERVER = "%s:%d".formatted(KAFKA_HOST, TLS_PORT);

    public static final String TOPIC = "acl-test";
}

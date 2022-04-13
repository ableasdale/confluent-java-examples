# confluent-java-examples

Mostly a work in progress at this stage.

## Consumers
- [TLSConsumer](src/main/java/io/confluent/cse/kafkaclient/consumer/TLSConsumer.java)
- [KerberosConsumer](src/main/java/io/confluent/cse/kafkaclient/consumer/KerberosConsumer.java)

## Producers
- [TLSProducer](src/main/java/io/confluent/cse/kafkaclient/producer/TLSProducer.java)
- [KerberosProducer](src/main/java/io/confluent/cse/kafkaclient/producer/KerberosProducer.java)

## Configuration

There are some files that need to go into the src/main/resources directory:

- admin.user.keytab (and any keytab files) 
- kafka.client.truststore.jks
- kafka.client.keystore.jks

There is also a [Config class for common values](src/main/java/io/confluent/cse/kafkaclient/Config.java)
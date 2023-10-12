package io.hstream.kafka.testing.Utils;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;

public class RawConsumerBuilder<K, V> {
  private final String hosts;
  private int maxPollRecords = 500;
  private String keyDeserializer = "org.apache.kafka.common.serialization.StringDeserializer";
  private String valueDeserializer = "org.apache.kafka.common.serialization.StringDeserializer";

  public RawConsumerBuilder(String hosts) {
    this.hosts = hosts;
  }

  public RawConsumerBuilder<K, V> maxPollRecords(int maxPollRecords) {
    this.maxPollRecords = maxPollRecords;
    return this;
  }

  public RawConsumerBuilder<K, V> keyDeserializer(String keyDeserializer) {
    this.keyDeserializer = keyDeserializer;
    return this;
  }

  public RawConsumerBuilder<K, V> valueDeserializer(String valueDeserializer) {
    this.valueDeserializer = valueDeserializer;
    return this;
  }

  public Consumer<K, V> build() {
    Properties props = new Properties();
    props.put("bootstrap.servers", hosts);
    props.put("key.deserializer", keyDeserializer);
    props.put("value.deserializer", valueDeserializer);
    props.put("max.poll.records", maxPollRecords);
    props.put("enable.auto.commit", false);
    return new KafkaConsumer<>(props);
  }
}

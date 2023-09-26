package io.hstream.kafka.testing.Utils;

import java.util.Properties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ConsumerBuilder<K, V> {
  private final String hosts;
  private String groupId = "group";
  private String autoOffsetReset = "earliest";
  private Boolean enableAutoCommit = true;
  private int maxPollRecords = 500;
  private String keyDeserializer = "org.apache.kafka.common.serialization.StringDeserializer";
  private String valueDeserializer = "org.apache.kafka.common.serialization.StringDeserializer";

  public ConsumerBuilder(String hosts) {
    this.hosts = hosts;
  }

  public ConsumerBuilder<K, V> groupId(String id) {
    this.groupId = id;
    return this;
  }

  public ConsumerBuilder<K, V> offsetReset(String offsetReset) {
    this.autoOffsetReset = offsetReset;
    return this;
  }

  public ConsumerBuilder<K, V> autoCommit(Boolean autoCommit) {
    this.enableAutoCommit = autoCommit;
    return this;
  }

  public ConsumerBuilder<K, V> maxPollRecords(int maxPollRecords) {
    this.maxPollRecords = maxPollRecords;
    return this;
  }

  public ConsumerBuilder<K, V> keyDeserializer(String keyDeserializer) {
    this.keyDeserializer = keyDeserializer;
    return this;
  }

  public ConsumerBuilder<K, V> valueDeserializer(String valueDeserializer) {
    this.valueDeserializer = valueDeserializer;
    return this;
  }

  public Consumer<K, V> build() {
    Properties props = new Properties();
    props.put("bootstrap.servers", hosts);
    props.put("key.deserializer", keyDeserializer);
    props.put("value.deserializer", valueDeserializer);
    props.put("group.id", groupId);
    props.put("enable.auto.commit", enableAutoCommit);
    props.put("auto.offset.reset", autoOffsetReset);
    props.put("max.poll.records", maxPollRecords);
    return new KafkaConsumer<>(props);
  }
}

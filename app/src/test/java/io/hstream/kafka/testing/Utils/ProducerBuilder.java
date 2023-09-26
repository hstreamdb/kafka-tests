package io.hstream.kafka.testing.Utils;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

public class ProducerBuilder<K, V> {
  private final String hosts;
  private String acks = "-1";
  private int retries = 3;
  private int batchSize = 16384;
  private int lingerMs = 0;
  private long bufferSize = 1024 * 1024L;
  private long blockMs = 60 * 1000L;
  private String keySerializer = "org.apache.kafka.common.serialization.StringSerializer";
  private String valueSerializer = "org.apache.kafka.common.serialization.StringSerializer";

  public ProducerBuilder(String hosts) {
    this.hosts = hosts;
  }

  public ProducerBuilder<K, V> acks(String acks) {
    this.acks = acks;
    return this;
  }

  public ProducerBuilder<K, V> retries(int retries) {
    this.retries = retries;
    return this;
  }

  public ProducerBuilder<K, V> batchSize(int batchSize) {
    this.batchSize = batchSize;
    return this;
  }

  public ProducerBuilder<K, V> lingerMs(int lingerMs) {
    this.lingerMs = lingerMs;
    return this;
  }

  public ProducerBuilder<K, V> bufferSize(long bufferSize) {
    this.bufferSize = bufferSize;
    return this;
  }

  public ProducerBuilder<K, V> blockMs(long blockMs) {
    this.blockMs = blockMs;
    return this;
  }

  public ProducerBuilder<K, V> keySerializer(String keySerializer) {
    this.keySerializer = keySerializer;
    return this;
  }

  public ProducerBuilder<K, V> valueSerializer(String valueSerializer) {
    this.valueSerializer = valueSerializer;
    return this;
  }

  public Producer<K, V> build() {
    Properties props = new Properties();
    props.put("bootstrap.servers", hosts);
    props.put("key.serializer", keySerializer);
    props.put("value.serializer", valueSerializer);
    props.put("acks", acks);
    props.put("retries", retries);
    props.put("batch.size", batchSize);
    props.put("linger.ms", lingerMs);
    props.put("buffer.memory", bufferSize);
    props.put("max.block.ms", blockMs);
    return new KafkaProducer<>(props);
  }
}

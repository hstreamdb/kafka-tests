package io.hstream.kafka.testing.Utils;

import static org.assertj.core.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Common {
  private static final Logger logger = LoggerFactory.getLogger(Common.class);

  private static final Random rand = new Random(System.currentTimeMillis());

  public static void createTopic(AdminClient client, String name, int partitions, short replica) {
    NewTopic requestedTopic = new NewTopic(name, partitions, replica);
    assertThatNoException()
        .as("create topics should success")
        .isThrownBy(
            () -> client.createTopics(List.of(requestedTopic)).all().get(5, TimeUnit.SECONDS));
  }

  // ============================ Producer =======================================
  public static Producer<byte[], byte[]> createByteProducer(String serverUrl) {
    return new ProducerBuilder<byte[], byte[]>(serverUrl)
        .keySerializer("org.apache.kafka.common.serialization.ByteArraySerializer")
        .valueSerializer("org.apache.kafka.common.serialization.ByteArraySerializer")
        .build();
  }

  public static List<ProducerRecord<byte[], byte[]>> sendBytesRecords(
      Producer<byte[], byte[]> producer, int numRecords, TopicPartition tp) {
    var records = new ArrayList<ProducerRecord<byte[], byte[]>>();
    for (int i = 0; i < numRecords; i++) {
      var record =
          new ProducerRecord<>(
              tp.topic(),
              tp.partition(),
              ("key " + i).getBytes(StandardCharsets.UTF_8),
              ("value " + i).getBytes(StandardCharsets.UTF_8));
      records.add(record);
      producer.send(record);
    }
    producer.flush();
    return records;
  }

  // ============================ Consumer =======================================

  public static Consumer<byte[], byte[]> createBytesConsumer(String serverUrl) {
    return new ConsumerBuilder<byte[], byte[]>(serverUrl)
        .keyDeserializer("org.apache.kafka.common.serialization.ByteArrayDeserializer")
        .valueDeserializer("org.apache.kafka.common.serialization.ByteArrayDeserializer")
        .build();
  }

  public static <K, V> List<ConsumerRecord<K, V>> consumeRecords(
      Consumer<K, V> consumer, int numRecords, long timeoutMs) {
    var records = pollUntilAtLeastNumRecords(consumer, numRecords, timeoutMs);
    assertThat(records).isNotNull();
    assertThat(records.size()).as("Consumed unexpected nums of records.").isEqualTo(numRecords);
    return records;
  }

  static <K, V> List<ConsumerRecord<K, V>> pollUntilAtLeastNumRecords(
      Consumer<K, V> consumer, int numRecords, long timeoutMs) {
    var records = new ArrayList<ConsumerRecord<K, V>>();
    pollRecordsUntilTrue(
        consumer,
        rs -> {
          records.addAll(rs.records(rs.partitions().iterator().next()));
          return records.size() >= numRecords;
        },
        () ->
            String.format(
                "Consumed %d records before timeout instead of the expected %d records",
                records.size(), numRecords),
        timeoutMs);
    return records;
  }

  static <K, V> void pollRecordsUntilTrue(
      Consumer<K, V> consumer,
      Function<ConsumerRecords<K, V>, Boolean> action,
      Supplier<String> msgSupplier,
      long waitTimeMs) {
    waitUntilTrue(
        () -> {
          ConsumerRecords<K, V> records = consumer.poll(100);
          return action.apply(records);
        },
        msgSupplier,
        waitTimeMs);
  }

  // ========================== helper ============================================

  /** Wait until the condition is true, or throw a timeout exception */
  static void waitUntilTrue(
      BooleanSupplier condition, Supplier<String> msgSupplier, long waitTimeMs) {
    try {
      CompletableFuture<Void> future =
          CompletableFuture.runAsync(
              () -> {
                while (true) {
                  if (condition.getAsBoolean()) {
                    return;
                  }
                  try {
                    Thread.sleep(100);
                  } catch (InterruptedException e) {
                    throw new RuntimeException("Interrupted", e);
                  }
                }
              });

      future.get(waitTimeMs, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      fail(msgSupplier.get());
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  public static String randomTopicName() {
    return "test_topic_" + UUID.randomUUID().toString().replace("-", "");
  }

  public static String randomTopicName(String prefix) {
    return prefix + "_topic_" + UUID.randomUUID().toString().replace("-", "");
  }

  public static void printBeginFlag(ExtensionContext context) {
    printFlag("begin", context);
  }

  public static void printEndFlag(ExtensionContext context) {
    printFlag("end", context);
  }

  private static void printFlag(String flag, ExtensionContext context) {
    logger.info(
        "=====================================================================================");
    logger.info(
        "{} {} {} {}",
        flag,
        context.getRequiredTestInstance().getClass().getSimpleName(),
        context.getTestMethod().get().getName(),
        context.getDisplayName());
    logger.info(
        "=====================================================================================");
  }
}

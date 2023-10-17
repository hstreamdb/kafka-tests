package io.hstream.kafka.testing;

import static io.hstream.kafka.testing.Utils.Common.*;
import static org.assertj.core.api.Assertions.*;

import io.hstream.kafka.testing.Utils.ProducerBuilder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import jdk.jfr.Description;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ExtendWith(ClusterExtension.class)
public class ProducerTest {
  private static final Logger logger = LoggerFactory.getLogger(ProducerTest.class);
  private String HStreamUrl = "127.0.0.1:9092";
  private AdminClient client;

  public void setHStreamUrl(String url) {
    this.HStreamUrl = url;
  }

  @BeforeEach
  void setUp() {
    Properties adminProps = new Properties();
    adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, HStreamUrl);
    try {
      client = AdminClient.create(adminProps);
    } catch (Exception e) {
      logger.error("create admin client failed: {}", e.toString());
      throw e;
    }
  }

  @AfterEach
  void tearDown() {
    client.close();
  }

  @Test
  @Timeout(40)
  void testSendOffsets() throws ExecutionException, InterruptedException, TimeoutException {
    var topic = randomTopicName("testSendOffset");
    final AtomicLong offset = new AtomicLong(0);
    Callback callBack =
        (metadata, exception) -> {
          if (exception == null) {
            assertThat(offset.get()).isEqualTo(metadata.offset());
            assertThat(topic).isEqualTo(metadata.topic());
            if (offset.get() == 0) {
              assertThat(metadata.serializedKeySize() + metadata.serializedValueSize())
                  .isEqualTo("key".length() + "value".length());
            } else if (offset.get() == 1) {
              assertThat(metadata.serializedKeySize()).isEqualTo("key".length());
            } else if (offset.get() == 2) {
              assertThat(metadata.serializedValueSize()).isEqualTo("value".length());
            } else {
              assertThat(metadata.serializedValueSize() > 0).isTrue();
            }
            offset.incrementAndGet();
          } else {
            fail("Send callback returns exceptions: %s", exception.toString());
          }
        };

    try {
      createTopic(client, topic, 1, (short) 2);
      var partition = 0;
      try (var producer = createByteProducer(HStreamUrl)) {
        var record0 =
            new ProducerRecord<>(
                topic,
                partition,
                "key".getBytes(StandardCharsets.UTF_8),
                "value".getBytes(StandardCharsets.UTF_8));
        assertThat(producer.send(record0, callBack).get(10, TimeUnit.SECONDS).offset())
            .as("send a normal record should success")
            .isEqualTo(0);

        var record1 =
            new ProducerRecord<byte[], byte[]>(
                topic, partition, "key".getBytes(StandardCharsets.UTF_8), null);
        assertThat(producer.send(record1, callBack).get(10, TimeUnit.SECONDS).offset())
            .as("send a record with null value should be ok")
            .isEqualTo(1);

        var record2 =
            new ProducerRecord<byte[], byte[]>(
                topic, partition, null, "value".getBytes(StandardCharsets.UTF_8));
        assertThat(producer.send(record2, callBack).get(10, TimeUnit.SECONDS).offset())
            .as("send a record with null key should be ok")
            .isEqualTo(2);

        var record3 =
            new ProducerRecord<>(
                topic,
                "key".getBytes(StandardCharsets.UTF_8),
                "value".getBytes(StandardCharsets.UTF_8));
        assertThat(producer.send(record3, callBack).get(10, TimeUnit.SECONDS).offset())
            .as("send a record with null partition id should be ok")
            .isEqualTo(3);

        // non-blocking send should success
        for (int i = 0; i < 100; i++) {
          assertThat(producer.send(record0, callBack).get(10, TimeUnit.SECONDS).offset())
              .isEqualTo(i + 4);
        }
      }
    } finally {
      client.deleteTopics(List.of(topic)).all().get();
    }
  }

  @Test
  @Timeout(30)
  @Description(
      "checks the closing behavior. After close() returns, all messages should be sent"
          + "with correct returned offset metadata")
  void testClose() throws ExecutionException, InterruptedException {
    var topic = randomTopicName("testClose");
    try {
      createTopic(client, topic, 1, (short) 2);
      var partition = 0;

      try (var producer = createByteProducer(HStreamUrl)) {
        var record0 =
            new ProducerRecord<>(
                topic,
                partition,
                "key".getBytes(StandardCharsets.UTF_8),
                "value".getBytes(StandardCharsets.UTF_8));
        for (int i = 0; i < 100; i++) {
          producer.send(record0);
        }
        var response = producer.send(record0);

        producer.close();

        assertThat(response)
            .as("The last message should be acked before producer is shutdown")
            .succeedsWithin(10, TimeUnit.SECONDS);
        assertThat(response.get().offset()).as("Should have offset 100").isEqualTo(100);
      }
    } finally {
      client.deleteTopics(List.of(topic)).all().get();
    }
  }

  @Test
  @Timeout(40)
  void testSendToPartition() throws RuntimeException, ExecutionException, InterruptedException {
    var topic = randomTopicName("testSendToPartition");
    try {
      createTopic(client, topic, 2, (short) 2);
      var partition = 1;
      try (var producer = createByteProducer(HStreamUrl);
          var consumer = createBytesConsumer(HStreamUrl)) {

        var futures = new ArrayList<Future<RecordMetadata>>();
        for (int i = 0; i < 100; i++) {
          futures.add(
              producer.send(
                  new ProducerRecord<>(
                      topic, partition, null, ("value" + i).getBytes(StandardCharsets.UTF_8))));
        }
        var metaDatas =
            futures.stream()
                .map(
                    f -> {
                      try {
                        return f.get(30, TimeUnit.SECONDS);
                      } catch (InterruptedException | ExecutionException | TimeoutException e) {
                        fail("get metadata fail: %s", e.toString());
                      }
                      return null;
                    })
                .collect(Collectors.toList());

        for (int i = 0; i < 100; i++) {
          assertThat(metaDatas.get(i).offset()).isEqualTo(i);
          assertThat(metaDatas.get(i).partition()).isEqualTo(partition);
          assertThat(metaDatas.get(i).topic()).isEqualTo(topic);
        }

        var tp = new TopicPartition(topic, partition);
        consumer.assign(List.of(tp));
        consumer.seekToBeginning(List.of(tp));
        var records = consumeRecords(consumer, 100, 30 * 1000);
        for (int i = 0; i < records.size(); i++) {
          assertThat(records.get(i).offset()).isEqualTo(i);
          assertThat(records.get(i).partition()).isEqualTo(partition);
          assertThat(records.get(i).topic()).isEqualTo(topic);
          assertThat(records.get(i).key()).isNull();
          assertThat(records.get(i).value())
              .isEqualTo(("value" + i).getBytes(StandardCharsets.UTF_8));
        }
      }
    } finally {
      client.deleteTopics(List.of(topic)).all().get();
    }
  }

  @Test
  @Description("flush will send all accumulated requests immediately")
  @Timeout(30)
  void testFlush() throws ExecutionException, InterruptedException, RuntimeException {
    var topic = randomTopicName("testFlush");

    try {
      createTopic(client, topic, 2, (short) 2);
      var recordCnt = 50;
      try (var producer =
          new ProducerBuilder<byte[], byte[]>(HStreamUrl)
              .lingerMs(Integer.MAX_VALUE)
              .keySerializer("org.apache.kafka.common.serialization.ByteArraySerializer")
              .valueSerializer("org.apache.kafka.common.serialization.ByteArraySerializer")
              .build()) {
        var record =
            new ProducerRecord<byte[], byte[]>(topic, "value".getBytes(StandardCharsets.UTF_8));
        var responses = new ArrayList<Future<RecordMetadata>>();
        for (int i = 0; i < recordCnt; i++) {
          responses.add(producer.send(record));
          responses.forEach(f -> assertThat(f.isDone()).as("No request is complete").isFalse());
          producer.flush();
          responses.forEach(f -> assertThat(f.isDone()).as("All requests are complete").isTrue());
          responses.clear();
        }
      }
    } finally {
      client.deleteTopics(List.of(topic)).all().get();
    }
  }
}

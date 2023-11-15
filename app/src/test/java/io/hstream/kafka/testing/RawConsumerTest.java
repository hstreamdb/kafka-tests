package io.hstream.kafka.testing;

import static io.hstream.kafka.testing.Utils.Common.*;

import io.hstream.kafka.testing.Utils.ConsumerBuilder;
import io.hstream.kafka.testing.Utils.RawConsumerBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Slf4j
@ExtendWith(ClusterExtension.class)
public class RawConsumerTest {
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
      log.error("create admin client failed: {}", e.toString());
      throw e;
    }
  }

  @AfterEach
  void tearDown() {
    client.close();
  }

  @Test
  void testMultiConsumerWithMultiPartitions() {
    var topic = randomTopicName("abc");
    var partitions = 3;
    createTopic(client, topic, partitions, (short) 1);
    var producer = createByteProducer(HStreamUrl);
    for (int i = 0; i < partitions; i++) {
      sendBytesRecords(producer, 10, new TopicPartition(topic, i));
    }

    var consumers = new ArrayList<Consumer<byte[], byte[]>>();
    for (int i = 0; i < partitions; i++) {
      var consumer = new RawConsumerBuilder<byte[], byte[]>(HStreamUrl).build();
      var tp = new TopicPartition(topic, i);
      consumer.assign(List.of(tp));
      consumer.seek(tp, 0);
      consumers.add(consumer);
    }
    var result = pollConcurrently(consumers, partitions * 10);
    for (int i = 0; i < partitions; i++) {
      Assertions.assertTrue(result.containsKey(new TopicPartition(topic, i)));
      Assertions.assertEquals(10, result.get(new TopicPartition(topic, i)).size());
      // TODO: check result data
    }
    consumers.forEach(Consumer::close);
  }

  @SneakyThrows
  @Test
  void testMultiProduceAndFetch() {
    var topic = randomTopicName("abc");
    createTopic(client, topic, 1, (short) 1);
    var producer = createByteProducer(HStreamUrl);
    var tp = new TopicPartition(topic, 0);
    sendBytesRecords(producer, 10, tp);

    var consumer1 =
            new RawConsumerBuilder<byte[], byte[]>(HStreamUrl).build();
    consumer1.assign(List.of(tp));
    consumer1.seekToBeginning(List.of(tp));
    consumeRecords(consumer1, 10, 10000);
    consumer1.close();
    log.info("finished the first round(produce and write)");

    // waiting before next-round write and read
    Thread.sleep(8000);

    sendBytesRecords(producer, 10, tp);
    log.info("wrote another 10 records");
    producer.close();

    var consumer2 =
            new RawConsumerBuilder<byte[], byte[]>(HStreamUrl).build();
    consumer2.assign(List.of(tp));
    consumer2.seek(tp, 10);
    consumeRecords(consumer2, 10, 10000);
    consumer2.close();
  }
}

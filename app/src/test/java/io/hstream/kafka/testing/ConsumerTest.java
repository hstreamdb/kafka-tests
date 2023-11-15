package io.hstream.kafka.testing;

import static io.hstream.kafka.testing.Utils.Common.*;

import io.hstream.kafka.testing.Utils.Common;
import io.hstream.kafka.testing.Utils.ConsumerBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;

@Slf4j
@ExtendWith(ClusterExtension.class)
public class ConsumerTest {
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

  List<Consumer<byte[], byte[]>> createConsumersAndPoll(
      String topic, String group, int consumerCount) {
    var consumers = createConsumers(topic, group, consumerCount);
    pollConcurrentlyWithPollCount(consumers, 1, 8000);
    return consumers;
  }

  @SneakyThrows
  List<Consumer<byte[], byte[]>> createConsumers(String topic, String group, int consumerCount) {
    var consumers = new ArrayList<Consumer<byte[], byte[]>>();
    AtomicBoolean success = new AtomicBoolean(true);
    for (int i = 0; i < consumerCount; i++) {
      var consumer = new ConsumerBuilder<byte[], byte[]>(HStreamUrl).groupId(group).build();
      consumers.add(consumer);
      consumer.subscribe(List.of(topic));
    }
    Assertions.assertTrue(success.get());
    return consumers;
  }

  @Test
  void testSingleConsumerWithEmptyTopic() throws Exception {
    var topic = randomTopicName("abc_topic_");
    createTopic(client, topic, 1, (short) 1);
    var consumers = createConsumersAndPoll(topic, "group01", 1);
    Common.assertAssignment(consumers, 1);
    consumers.forEach(Consumer::close);
  }

  @Test
  void testSingleConsumerWithEmptyTopicAndMultiPartitions() throws Exception {
    var topic = randomTopicName("abc_topic_");
    createTopic(client, topic, 3, (short) 1);
    var consumers = createConsumersAndPoll(topic, "group01", 1);
    Common.assertAssignment(consumers, 3);
    consumers.forEach(Consumer::close);
  }

  @Test
  void testMultiConsumerWithEmptyTopicAndSinglePartition() throws Exception {
    var group = "group01";
    var topic = randomTopicName("abc_topic_");
    createTopic(client, topic, 1, (short) 1);
    var consumers = createConsumersAndPoll(topic, group, 3);
    Common.assertAssignment(consumers, 1);
    consumers.forEach(Consumer::close);
  }

  @Test
  void testMultiConsumerWithEmptyTopicAndMultiPartitions() throws Exception {
    var group = "group01";
    var topic = randomTopicName("abc_topic_");
    createTopic(client, topic, 3, (short) 1);
    var consumers = createConsumersAndPoll(topic, group, 3);
    Common.assertAssignment(consumers, 3);
    Common.assertBalancedAssignment(consumers, 3);
    consumers.forEach(Consumer::close);
  }

  @Test
  void testJoinGroupRebalance() throws Exception {
    var group = "group01";
    var topic = randomTopicName("abc_topic_");
    createTopic(client, topic, 3, (short) 1);
    var consumers = createConsumersAndPoll(topic, group, 2);
    log.info("first phase assignment:");
    Common.assertAssignment(consumers, 3);

    var newConsumer = new ConsumerBuilder<byte[], byte[]>(HStreamUrl).groupId(group).build();
    newConsumer.subscribe(List.of(topic));
    consumers.add(newConsumer);

    pollConcurrentlyWithPollCount(consumers, 1, 8000);
    log.info("rebalanced assignment:");
    Common.assertAssignment(consumers, 3);
    Common.assertBalancedAssignment(consumers, 3);
    consumers.forEach(Consumer::close);
  }

  @Test
  void testSingleConsumer() {
    var group = "group01";
    var topic = randomTopicName("abc_topic_");
    createTopic(client, topic, 1, (short) 1);
    var producer = createByteProducer(HStreamUrl);
    sendBytesRecords(producer, 10, new TopicPartition(topic, 0));
    producer.close();

    var consumer = createConsumers(topic, group, 1).get(0);
    consumer.subscribe(List.of(topic));
    var records = consumeRecords(consumer, 10, 10000);
    Assertions.assertEquals(10, records.size());
    consumer.close();
  }

  @Test
  void testSingleConsumerWithMultiPartitions() throws Exception {
    var topic = randomTopicName("abc_topic_");
    var partitions = 3;
    createTopic(client, topic, partitions, (short) 1);
    try (var producer = createByteProducer(HStreamUrl)) {
      for (int i = 0; i < partitions; i++) {
        sendBytesRecords(producer, 10, new TopicPartition(topic, i));
      }
    }

    var consumers = createConsumers(topic, "group01", 1);
    var result = pollConcurrently(consumers, partitions * 10);
    Common.assertAssignment(consumers, 3);
    for (int i = 0; i < partitions; i++) {
      var tp = new TopicPartition(topic, i);
      Assertions.assertNotNull(result.get(tp));
      Assertions.assertEquals(10, result.get(tp).size());
    }
    consumers.forEach(Consumer::close);
  }

  @Test
  void testMultiConsumerWithSinglePartition() throws Exception {
    var topic = randomTopicName("abc_topic_");
    createTopic(client, topic, 1, (short) 1);
    var producer = createByteProducer(HStreamUrl);
    sendBytesRecords(producer, 10, new TopicPartition(topic, 0));
    producer.close();

    var consumers = createConsumers(topic, "group01", 3);
    var result = pollConcurrently(consumers, 10);
    Common.assertAssignment(consumers, 1);
    Assertions.assertEquals(10, result.get(new TopicPartition(topic, 0)).size());
    // TODO: check result data
    consumers.forEach(Consumer::close);
  }

  @Test
  void testMultiConsumerWithMultiPartitions() {
    var topic = randomTopicName("abc_topic_");
    var partitions = 3;
    createTopic(client, topic, partitions, (short) 1);
    var producer = createByteProducer(HStreamUrl);
    for (int i = 0; i < partitions; i++) {
      sendBytesRecords(producer, 10, new TopicPartition(topic, i));
    }
    producer.close();

    var consumers = createConsumers(topic, "group01", 3);
    var result = pollConcurrently(consumers, 30);
    Common.assertAssignment(consumers, 3);
    Common.assertBalancedAssignment(consumers, 3);
    for (int i = 0; i < partitions; i++) {
      Assertions.assertEquals(10, result.get(new TopicPartition(topic, i)).size());
      // TODO: check result data
    }
    consumers.forEach(Consumer::close);
  }

  // also tested leave group
  @Test
  void testCommitAndFetchOffsets() throws Exception {
    var group = "group01";
    var topic = randomTopicName("abc");
    createTopic(client, topic, 1, (short) 1);
    var producer = createByteProducer(HStreamUrl);
    var tp = new TopicPartition(topic, 0);
    sendBytesRecords(producer, 10, tp);

    var consumer1 =
        new ConsumerBuilder<byte[], byte[]>(HStreamUrl).groupId(group).autoCommit(false).build();
    consumer1.subscribe(List.of(topic));
    var records = consumeRecords(consumer1, 10, 10000);
    Assertions.assertEquals(10, records.size());

    log.info("committing offsets");
    consumer1.commitSync();
    log.info("committed offsets");
    consumer1.close();
    log.info("closed consumer1");

    // waiting server to handle leave group and re-balance
    Thread.sleep(8000);

    sendBytesRecords(producer, 10, tp);
    log.info("wrote another 10 records");
    producer.close();

    var consumer2 =
        new ConsumerBuilder<byte[], byte[]>(HStreamUrl).groupId(group).autoCommit(false).build();
    consumer2.subscribe(List.of(topic));
    var newRecords = consumeRecords(consumer2, 10, 10000);
    Assertions.assertEquals(10, newRecords.size());
    consumer2.commitSync();
    var offsets = consumer2.endOffsets(List.of(tp));
    log.info("current offsets: {}", offsets);
    consumer2.close();
  }

  @Test
  void testManualAssign() throws Exception {
    var group = "group01";
    var topic = randomTopicName("abc");
    createTopic(client, topic, 1, (short) 1);
    var producer = createByteProducer(HStreamUrl);
    var tp = new TopicPartition(topic, 0);
    sendBytesRecords(producer, 10, tp);

    var consumer1 =
        new ConsumerBuilder<byte[], byte[]>(HStreamUrl).groupId(group).autoCommit(false).build();
    consumer1.assign(List.of(tp));
    consumeRecords(consumer1, 10, 10000);

    log.info("committing offsets");
    consumer1.commitSync();
    log.info("committed offsets");
    consumer1.close();
    log.info("closed consumer1");

    // waiting server to handle leave group and re-balance
    Thread.sleep(8000);

    sendBytesRecords(producer, 10, tp);
    log.info("wrote another 10 records");
    producer.close();

    var consumer2 =
        new ConsumerBuilder<byte[], byte[]>(HStreamUrl).groupId(group).autoCommit(false).build();
    consumer2.subscribe(List.of(topic));
    consumeRecords(consumer2, 10, 10000);
    consumer2.commitSync();
    var offsets = consumer2.endOffsets(List.of(tp));
    log.info("current offsets: {}", offsets);
    consumer2.close();
  }

  @Test
  void testManualSeek() throws Exception {
    var group = "group01";
    var topic = randomTopicName("abc");
    createTopic(client, topic, 1, (short) 1);
    var producer = createByteProducer(HStreamUrl);
    var tp = new TopicPartition(topic, 0);
    sendBytesRecords(producer, 10, tp);

    var consumer1 =
        new ConsumerBuilder<byte[], byte[]>(HStreamUrl).groupId(group).autoCommit(false).build();
    consumer1.assign(List.of(tp));
    consumer1.seekToBeginning(List.of(tp));
    consumeRecords(consumer1, 10, 10000);

    log.info("committing offsets");
    consumer1.commitSync();
    log.info("committed offsets");
    consumer1.close();
    log.info("closed consumer1");

    // waiting server to handle leave group and re-balance
    Thread.sleep(8000);

    sendBytesRecords(producer, 10, tp);
    log.info("wrote another 10 records");
    producer.close();

    var consumer2 =
        new ConsumerBuilder<byte[], byte[]>(HStreamUrl).groupId(group).autoCommit(false).build();
    consumer2.subscribe(List.of(topic));
    consumeRecords(consumer2, 10, 10000);
    consumer2.commitSync();
    var offsets = consumer2.endOffsets(List.of(tp));
    log.info("current offsets: {}", offsets);
    consumer2.close();
  }
}

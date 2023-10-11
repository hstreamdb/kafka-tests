package io.hstream.kafka.testing;

import static io.hstream.kafka.testing.Utils.Common.*;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import io.hstream.kafka.testing.Utils.Common;
import io.hstream.kafka.testing.Utils.ConsumerBuilder;
import lombok.SneakyThrows;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ExtendWith(ClusterExtension.class)
public class ConsumerTest {
  private static final Logger logger = LoggerFactory.getLogger(ConsumerTest.class);
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

  List<Consumer<byte[], byte[]>> createConsumersAndPoll(String topic, String group, int consumerCount) {
    var consumers = createConsumers(topic, group, consumerCount);
    pollConcurrently(consumers, 8000);
    return consumers;
  }

  @SneakyThrows
  List<Consumer<byte[], byte[]>> createConsumers(String topic, String group, int consumerCount) {
    var consumers = new LinkedList<Consumer<byte[], byte[]>>();
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
  @Timeout(40)
  void testSingleConsumerWithEmptyTopic() throws Exception {
    var topic = randomTopicName("abc_topic_");
    createTopic(client, topic, 1, (short) 1);
    var consumers = createConsumersAndPoll(topic, "group01", 1);
    Common.assertAssignment(consumers, 1);
  }

  @Test
  @Timeout(40)
  void testSingleConsumerWithEmptyTopicAndMultiPartitions() throws Exception {
    var topic = randomTopicName("abc_topic_");
    createTopic(client, topic, 3, (short) 1);
    var consumers = createConsumersAndPoll(topic, "group01", 1);
    Common.assertAssignment(consumers, 3);
  }

  @Test
  @Timeout(40)
  void testMultiConsumerWithEmptyTopicAndSinglePartition() throws Exception {
    var group = "group01";
    var topic = randomTopicName("abc_topic_");
    createTopic(client, topic, 1, (short) 1);
    var consumers = createConsumersAndPoll(topic, group, 3);
    Common.assertAssignment(consumers, 1);
  }

  @Test
  @Timeout(40)
  void testMultiConsumerWithEmptyTopicAndMultiPartitions() throws Exception {
    var group = "group01";
    var topic = randomTopicName("abc_topic_");
    createTopic(client, topic, 3, (short) 1);
    var consumers = createConsumersAndPoll(topic, group, 3);
    Common.assertAssignment(consumers, 3);
    Common.assertBalancedAssignment(consumers, 3);
  }

  @Test
  @Timeout(40)
  void testJoinGroupRebalance() throws Exception {
    var group = "group01";
    var topic = randomTopicName("abc_topic_");
    createTopic(client, topic, 3, (short) 1);
    var consumers = createConsumersAndPoll(topic, group, 2);
    logger.info("first phase assignment:");
    Common.assertAssignment(consumers, 3);

    var newConsumer = new ConsumerBuilder<byte[], byte[]>(HStreamUrl).groupId(group).build();
    newConsumer.subscribe(List.of(topic));
    consumers.add(newConsumer);

    pollConcurrently(consumers, 2000, 5);
    logger.info("rebalanced assignment:");
    Common.assertAssignment(consumers, 3);
    Common.assertBalancedAssignment(consumers, 3);
  }

  @Test
  @Timeout(40)
  void testSingleConsumer() {
    var group = "group01";
    var topic = randomTopicName("abc_topic_");
    createTopic(client, topic, 1, (short) 1);
    var producer = createByteProducer(HStreamUrl);
    sendBytesRecords(producer, 10, new TopicPartition(topic, 0));

    var consumer = createConsumers(topic, group, 1).get(0);
    consumer.subscribe(List.of(topic));
    var records = consumer.poll(10000);
    Assertions.assertEquals(10, records.count());
  }

  @Test
  @Timeout(40)
  void testSingleConsumerWithMultiPartitions() throws Exception {
    var topic = randomTopicName("abc_topic_");
    var partitions = 3;
    createTopic(client, topic, partitions, (short) 1);
    var producer = createByteProducer(HStreamUrl);
    for (int i = 0; i < partitions; i++) {
      sendBytesRecords(producer, 10, new TopicPartition(topic, i));
    }

    var consumers = createConsumers(topic, "group01", 1);
    var result = pollConcurrently(consumers, 8000);
    Common.assertAssignment(consumers, 3);
    for (int i = 0; i < partitions; i++) {
      Assertions.assertEquals(10, result.get(new TopicPartition(topic, i)).size());
      // TODO: check result data
    }
  }

  @Test
  @Timeout(40)
  void testMultiConsumerWithSinglePartition() throws Exception {
    var topic = randomTopicName("abc_topic_");
    createTopic(client, topic, 1, (short) 1);
    var producer = createByteProducer(HStreamUrl);
    sendBytesRecords(producer, 10, new TopicPartition(topic, 0));

    var consumers = createConsumers(topic, "group01", 3);
    var result = pollConcurrently(consumers, 8000);
    Common.assertAssignment(consumers, 1);
    Assertions.assertEquals(10, result.get(new TopicPartition(topic, 0)).size());
    // TODO: check result data
  }

  @Test
  @Timeout(40)
  void testMultiConsumerWithMultiPartitions() {
    var topic = randomTopicName("abc_topic_");
    var partitions = 3;
    createTopic(client, topic, partitions, (short) 1);
    var producer = createByteProducer(HStreamUrl);
    for (int i = 0; i < partitions; i++) {
      sendBytesRecords(producer, 10, new TopicPartition(topic, i));
    }

    var consumers = createConsumers(topic, "group01", 3);
    var result = pollConcurrently(consumers, 8000);
    Common.assertAssignment(consumers, 3);
    Common.assertBalancedAssignment(consumers, 3);
    for (int i = 0; i < partitions; i++) {
      Assertions.assertEquals(10, result.get(new TopicPartition(topic, i)).size());
      // TODO: check result data
    }
  }
}

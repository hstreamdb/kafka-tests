package io.hstream.kafka.testing.version_3_5;

import io.hstream.kafka.testing.ClusterExtension;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.hstream.kafka.testing.Utils.Common.*;

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

   @Test
   void testListGroups() throws Exception {
     var group = "group01";
     var topic = randomTopicName("abc_topic_");
     createTopic(client, topic, 1, (short) 1);
     var producer = createByteProducer(HStreamUrl);
     sendBytesRecords(producer, 10, new TopicPartition(topic, 0));

     var consumer = createConsumers(HStreamUrl, topic, group, 1).get(0);
     consumer.subscribe(List.of(topic));
     var records = consumeRecords(consumer, 10, 10000);
     Assertions.assertEquals(10, records.size());

     // list group
     var groups = client.listConsumerGroups().all().get().stream().collect(Collectors.toList());
     Assertions.assertEquals(1, groups.size());
     Assertions.assertEquals(group, groups.get(0).groupId());
   }

   @Test
   void testDescribeGroups() throws Exception {
     var group = "group01";
     var topic = randomTopicName("abc_topic_");
     createTopic(client, topic, 1, (short) 1);
     var producer = createByteProducer(HStreamUrl);
     sendBytesRecords(producer, 10, new TopicPartition(topic, 0));

     var consumer = createConsumers(HStreamUrl, topic, group, 1).get(0);
     consumer.subscribe(List.of(topic));
     var records = consumeRecords(consumer, 10, 10000);
     Assertions.assertEquals(10, records.size());

     // list group
     var groups = client.describeConsumerGroups(List.of(group)).all().get();
     Assertions.assertEquals(1, groups.size());
     Assertions.assertEquals(group, groups.get(group).groupId());
     Assertions.assertEquals(1, groups.get(group).members().size());
   }

   @Test
   void testFetchOffsets() throws Exception {
     var group = "group01";
     var topic = randomTopicName("abc_topic_");
     createTopic(client, topic, 1, (short) 1);
     var producer = createByteProducer(HStreamUrl);
     sendBytesRecords(producer, 10, new TopicPartition(topic, 0));

     var consumer = createConsumers(HStreamUrl, topic, group, 1).get(0);
     consumer.subscribe(List.of(topic));
     var records = consumeRecords(consumer, 10, 10000);
     Assertions.assertEquals(10, records.size());

     // list group
     var groupOffsets = client.listConsumerGroupOffsets(group).all().get(5, TimeUnit.SECONDS);
     Assertions.assertEquals(1, groupOffsets.size());
     log.info("group offsets:{}", groupOffsets);
   }
}

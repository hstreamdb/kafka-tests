package io.hstream.kafka.testing.version_3_5;

import static io.hstream.kafka.testing.Utils.Common.*;

import io.hstream.kafka.testing.ClusterExtension;
import java.util.Map;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@Slf4j
@ExtendWith(ClusterExtension.class)
public class AdminTest {
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
  void testAdminListOffsets() throws Exception {
    var topic = randomTopicName("abc_topic_");
    var tp = new TopicPartition(topic, 0);
    createTopic(client, topic, 1, (short) 1);
    var producer = createByteProducer(HStreamUrl);
    sendBytesRecords(producer, 10, new TopicPartition(topic, 0));

    var earliest = client.listOffsets(Map.of(tp, OffsetSpec.earliest())).partitionResult(tp).get();
    var latest = client.listOffsets(Map.of(tp, OffsetSpec.latest())).partitionResult(tp).get();
    Assertions.assertEquals(0, earliest.offset());
    Assertions.assertEquals(10, latest.offset());
  }
}

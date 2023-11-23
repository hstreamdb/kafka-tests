package io.hstream.kafka.testing;

import static io.hstream.kafka.testing.Utils.Common.*;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@Slf4j
@ExtendWith(ClusterExtension.class)
public class ConfigTest {
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
  void testDescribeConfigs() throws ExecutionException, InterruptedException {
    var topic = randomTopicName();
    var partitions = 3;
    createTopic(client, topic, partitions, (short) 1);
    var cr = new ConfigResource(ConfigResource.Type.TOPIC, topic);
    var result = client.describeConfigs(List.of(cr)).all().get();
    log.info("describeConfigs result:{}", result);
    Assertions.assertEquals(1, result.size());
    Assertions.assertNotNull(result.get(cr));
    Assertions.assertNotNull(result.get(cr).get("cleanup.policy"));
    Assertions.assertNotNull(result.get(cr).get("retention.ms"));
  }

  @Test
  void testDescribeBrokerConfigs() throws ExecutionException, InterruptedException {
    var nodes = client.describeCluster().nodes().get().stream().collect(Collectors.toList());
    Assertions.assertFalse(nodes.isEmpty());
    var node = nodes.get(0);
    var cr = new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(node.id()));
    var result = client.describeConfigs(List.of(cr)).all().get();
    log.info("describeConfigs result:{}", result);
    Assertions.assertEquals(1, result.size());
    Assertions.assertNotNull(result.get(cr));
    Assertions.assertNotNull(result.get(cr).get("auto.create.topics.enable"));
    Assertions.assertEquals("true", result.get(cr).get("auto.create.topics.enable").value());
  }

  @Test
  void testRetentionMs() throws ExecutionException, InterruptedException {
    var topic = randomTopicName();
    var partitions = 3;
    var retentionMs = "3600000";
    createTopic(client, topic, partitions, (short) 1, Map.of("retention.ms", retentionMs));
    var cr = new ConfigResource(ConfigResource.Type.TOPIC, topic);
    var result = client.describeConfigs(List.of(cr)).all().get();
    log.info("describeConfigs result:{}", result);
    Assertions.assertEquals(1, result.size());
    Assertions.assertNotNull(result.get(cr));
    Assertions.assertNotNull(result.get(cr).get("cleanup.policy"));
    Assertions.assertNotNull(result.get(cr).get("retention.ms"));
    Assertions.assertEquals(retentionMs, result.get(cr).get("retention.ms").value());
  }
}

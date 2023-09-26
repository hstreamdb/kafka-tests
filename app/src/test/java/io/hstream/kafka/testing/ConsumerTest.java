package io.hstream.kafka.testing;

import static io.hstream.kafka.testing.Utils.Common.*;
import static org.assertj.core.api.Assertions.*;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
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

  @Test
  @Timeout(40)
  void testSimpleConsumption() throws ExecutionException, InterruptedException, TimeoutException {
    var numRecords = 10000;
    var topic = randomTopicName("testSimpleConsumption");

    try (var producer = createByteProducer(HStreamUrl);
        var consumer = createBytesConsumer(HStreamUrl)) {
      try {
        createTopic(client, topic, 2, (short) 2);

        var tp = new TopicPartition(topic, 0);
        sendBytesRecords(producer, numRecords, tp);

        assertThat(consumer.assignment()).isEmpty();
        assertThatNoException().isThrownBy(() -> consumer.assign(List.of(tp)));
        assertThat(consumer.assignment()).containsExactly(tp);
      } finally {
        client.deleteTopics(List.of(topic)).all().get(10, TimeUnit.SECONDS);
      }
    }
  }
}

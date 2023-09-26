package io.hstream.kafka.testing;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ExtendWith(ClusterExtension.class)
public class TopicTest {
  private static final Logger logger = LoggerFactory.getLogger(TopicTest.class);
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
  @Timeout(10)
  void testCreateTopic() {
    NewTopic requestedTopic1 = new NewTopic("test_create_topic1", 1, (short) 1);
    NewTopic requestedTopic2 = new NewTopic("test_create_topic2", 2, (short) 3);
    try {
      assertThatNoException()
          .as("create topics should success")
          .isThrownBy(
              () -> client.createTopics(List.of(requestedTopic1, requestedTopic2)).all().get());
      assertThatThrownBy(
              () -> client.createTopics(Collections.singleton(requestedTopic1)).all().get())
          .as("create topics with same name should fail")
          .isInstanceOf(ExecutionException.class);

    } finally {
      assertThatNoException()
          .isThrownBy(
              () ->
                  client
                      .deleteTopics(List.of("test_create_topic1", "test_create_topic2"))
                      .all()
                      .get());
    }
  }
}

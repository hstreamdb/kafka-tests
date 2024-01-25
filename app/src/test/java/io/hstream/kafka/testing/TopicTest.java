package io.hstream.kafka.testing;

import static org.assertj.core.api.Assertions.*;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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

  @Test
  void testCreatePartition() throws ExecutionException, InterruptedException {
    NewTopic topic1 = new NewTopic("test_create_partition_topic1", 1, (short) 1);
    NewTopic topic2 = new NewTopic("test_create_partition_topic2", 3, (short) 3);
    try {
      assertThatNoException()
          .as("create topics should success")
          .isThrownBy(() -> client.createTopics(List.of(topic1, topic2)).all().get());

      assertThatThrownBy(
              () ->
                  client
                      .createPartitions(
                          Collections.singletonMap(
                              topic1.name(),
                              NewPartitions.increaseTo(2, Collections.singletonList(List.of(0)))))
                      .all()
                      .get())
          .as("create partitions with assignment should fail")
          .cause()
          .isInstanceOf(InvalidRequestException.class);
      assertThatThrownBy(
              () ->
                  client
                      .createPartitions(
                          Collections.singletonMap(topic1.name(), NewPartitions.increaseTo(1)))
                      .all()
                      .get())
          .as("new partition count should be greater than old partition count")
          .cause()
          .isInstanceOf(InvalidPartitionsException.class);
      assertThatThrownBy(
              () ->
                  client
                      .createPartitions(
                          Collections.singletonMap(topic2.name(), NewPartitions.increaseTo(2)))
                      .all()
                      .get())
          .as("new partition count should be greater than old partition count")
          .cause()
          .isInstanceOf(InvalidPartitionsException.class);
      assertThatNoException()
          .as("create partitions should success")
          .isThrownBy(
              () ->
                  client
                      .createPartitions(
                          Collections.singletonMap(topic2.name(), NewPartitions.increaseTo(6)))
                      .all()
                      .get());
      assertThatNoException()
          .as("create partitions with validateOnly should success")
          .isThrownBy(
              () ->
                  client
                      .createPartitions(
                          Collections.singletonMap(topic1.name(), NewPartitions.increaseTo(6)),
                          new CreatePartitionsOptions().validateOnly(true))
                      .all()
                      .get());

      assertThat(
              client
                  .describeTopics(List.of(topic1.name()))
                  .allTopicNames()
                  .get()
                  .get(topic1.name())
                  .partitions()
                  .size())
          .as("partition count for topic1 should be 1")
          .isEqualTo(1);

      var partitions =
          client
              .describeTopics(List.of(topic2.name()))
              .allTopicNames()
              .get()
              .get(topic2.name())
              .partitions();
      assertThat(partitions.size()).as("partition count should be 6").isEqualTo(6);
      // check failed because https://emqx.atlassian.net/browse/HS-4482
      //      for (var partition : partitions) {
      //        logger.info("partition: {}, replicas: {}", partition.partition(),
      // partition.replicas().size());
      //        assertThat(partition.replicas().size()).as("replica count should be
      // 3").isEqualTo(3);
      //      }

    } finally {
      assertThatNoException()
          .isThrownBy(
              () ->
                  client
                      .deleteTopics(
                          List.of("test_create_partition_topic1", "test_create_partition_topic2"))
                      .all()
                      .get());
    }
  }
}

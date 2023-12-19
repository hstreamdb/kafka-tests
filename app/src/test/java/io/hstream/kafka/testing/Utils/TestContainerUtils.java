package io.hstream.kafka.testing.Utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

public class TestContainerUtils {
  private static final Logger logger = LoggerFactory.getLogger(TestContainerUtils.class);
  private static final Network kafkaTest = Network.newNetwork();
  private static final String zkImage = "zookeeper:3.8";
  private static final String rqliteImage = "rqlite/rqlite";
  private static final String hstoreImage = "hstreamdb/hstream:latest";
  private static final String hsImage = "hstreamdb/hstream:latest";

  public static GenericContainer<?> makeZooKeeper() {
    return new GenericContainer<>(DockerImageName.parse(zkImage))
        .withNetwork(kafkaTest)
        .withNetworkAliases("zookeeper");
  }

  public static GenericContainer<?> makeRQLite() {
    return new GenericContainer<>(DockerImageName.parse(rqliteImage))
        .withNetwork(kafkaTest)
        .withNetworkAliases("rqlite");
  }

  public static GenericContainer<?> makeHStore(Path dataDir) {
    return new GenericContainer<>(DockerImageName.parse(hstoreImage))
        .withNetwork(kafkaTest)
        .withNetworkAliases("hstore")
        .withFileSystemBind(
            dataDir.toAbsolutePath().toString(), "/kafka-data/hstore", BindMode.READ_WRITE)
        .withCommand(
            "bash",
            "-c",
            "ld-dev-cluster "
                + "--root /kafka-data/hstore "
                + "--use-tcp "
                + "--tcp-host "
                + "$(hostname -I | awk '{print $1}') "
                + "--user-admin-port 6440 "
                + "--no-interactive")
        .waitingFor(Wait.forLogMessage(".*LogDevice Cluster running.*", 1));
  }

  public static FixedHostPortGenericContainer<?> makeHServer(
      HServerCliOpts hserverConf, String seedNodes, Path dataDir) {
    return new FixedHostPortGenericContainer<>(getHStreamImageName().toString())
        .withNetwork(kafkaTest)
        .withNetworkAliases("hserver" + hserverConf.serverId)
        .withFixedExposedPort(hserverConf.port, hserverConf.port)
        .withExposedPorts(hserverConf.port)
        .withFileSystemBind(
            dataDir.toAbsolutePath().toString(), "/kafka-data/hstore", BindMode.READ_ONLY)
        .withCommand(
            "bash", "-c", " hstream-server kafka" + hserverConf + " --seed-nodes " + seedNodes)
        // .withLogConsumer(msg -> logger.info(msg.getUtf8String()))
        .waitingFor(Wait.forLogMessage(".*Server is started on port.*", 1));
  }

  public static class HServerCliOpts {
    public int serverId;
    public String address;
    public int port;
    public int gossipPort;
    public String metaHost;

    public String toString() {
      return " --bind-address "
          + "0.0.0.0 "
          + " --port "
          + port
          + " --gossip-port "
          + gossipPort
          + " --advertised-address "
          + address
          + " --gossip-address $(hostname -I | awk '{print $1}')"
          + " --server-id "
          + serverId
          + " --metastore-uri "
          + getHStreamMetaStorePreference(metaHost)
          + " --store-config "
          + "/kafka-data/hstore/logdevice.conf "
          + " --log-level "
          + "debug"
          + " --log-with-color"
          + " --log-flush-immediately"
          + " --store-log-level "
          + "error";
    }
  }

  public static List<GenericContainer<?>> bootstrapHServerCluster(
      List<HServerCliOpts> hserverConfs, List<String> hserverInnerUrls, Path dataDir)
      throws IOException, InterruptedException {
    List<GenericContainer<?>> hservers = new ArrayList<>();
    var seedNodes = String.join(",", hserverInnerUrls);
    logger.info("seedNodes: {}", seedNodes);
    for (HServerCliOpts hserverConf : hserverConfs) {
      var hserver = makeHServer(hserverConf, seedNodes, dataDir);
      hservers.add(hserver);
    }
    hservers.stream().parallel().forEach(GenericContainer::start);
    var res =
        hservers
            .get(0)
            .execInContainer(
                "bash",
                "-c",
                "hstream-kafka-cli --host 127.0.0.1 "
                    // + hserverConfs.get(0).address
                    + " --port "
                    + hserverConfs.get(0).port
                    + " node init ");
    logger.info("init res:{}, {}, {}", res.getExitCode(), res.getStdout(), res.getStderr());
    return hservers;
  }

  private static String getHStreamMetaStorePreference(String metaHost) {
    String hstreamMetaStore = System.getenv("HSTREAM_META_STORE");
    if (hstreamMetaStore == null
        || hstreamMetaStore.isEmpty()
        || hstreamMetaStore.equalsIgnoreCase("ZOOKEEPER")) {
      logger.info("Use default Zookeeper HSTREAM_META_STORE");
      return "zk://zookeeper:2181";
    } else if (hstreamMetaStore.equalsIgnoreCase("RQLITE")) {
      logger.info("HSTREAM_META_STORE specified RQLITE as meta store");
      return "rq://rqlite:4001";
    } else {
      throw new RuntimeException("Invalid HSTREAM_META_STORE env variable value");
    }
  }

  private static DockerImageName getHStreamImageName() {
    String hstreamImageName = System.getenv("HSTREAM_IMAGE_NAME");
    if (hstreamImageName == null || hstreamImageName.isEmpty()) {
      logger.info("No env variable HSTREAM_IMAGE_NAME found, use default name {}", hsImage);
      hstreamImageName = hsImage;
    } else {
      logger.info("Found env variable HSTREAM_IMAGE_NAME = {}", hstreamImageName);
    }
    return DockerImageName.parse(hstreamImageName);
  }

  public static void writeLog(ExtensionContext context, String entryName, String grp, String logs)
      throws Exception {
    String testClassName = context.getRequiredTestClass().getSimpleName();
    String testName = context.getTestMethod().get().getName();
    String filePathFromProject =
        ".logs/" + testClassName + "/" + testName + "/" + grp + "/" + entryName;
    logger.info("log to " + filePathFromProject);
    String fileName = "../" + filePathFromProject;

    File file = new File(fileName);
    file.getParentFile().mkdirs();
    BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));
    writer.write(logs);
    writer.close();
  }
}

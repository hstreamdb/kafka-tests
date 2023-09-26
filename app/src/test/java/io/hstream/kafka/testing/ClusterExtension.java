package io.hstream.kafka.testing;

import static io.hstream.kafka.testing.Utils.Common.*;
import static io.hstream.kafka.testing.Utils.TestContainerUtils.*;

import io.hstream.kafka.testing.Utils.TestContainerUtils;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

public class ClusterExtension implements BeforeEachCallback, AfterEachCallback {

  static final int CLUSTER_SIZE = 3;
  private static final AtomicInteger count = new AtomicInteger(0);
  private static final Logger logger = LoggerFactory.getLogger(ClusterExtension.class);
  private final List<GenericContainer<?>> hservers = new ArrayList<>(CLUSTER_SIZE);
  private final List<String> hserverUrls = new ArrayList<>(CLUSTER_SIZE);
  private final List<String> hserverInnerUrls = new ArrayList<>(CLUSTER_SIZE);
  private GenericContainer<?> zk;
  private GenericContainer<?> rq;
  private GenericContainer<?> hstore;
  private String grp;
  private long beginTime;

  @Override
  public void beforeEach(ExtensionContext context) throws Exception {
    beginTime = System.currentTimeMillis();

    grp = UUID.randomUUID().toString();
    printBeginFlag(context);

    var dataDir = Files.createTempDirectory("hstream-kafka");

    zk = makeZooKeeper();
    zk.start();
    rq = makeRQLite();
    rq.start();
    String metaHost = "127.0.0.1";

    hstore = makeHStore(dataDir);
    hstore.start();
    String hstoreHost = hstore.getHost();
    logger.info("hstoreHost: " + hstoreHost);

    List<TestContainerUtils.HServerCliOpts> hserverConfs = new ArrayList<>(CLUSTER_SIZE);
    for (int i = 0; i < CLUSTER_SIZE; ++i) {
      int offset = count.getAndIncrement();
      int hserverPort = 9092 + offset;
      int hserverGossipPort = 65000 + offset;
      TestContainerUtils.HServerCliOpts options = new TestContainerUtils.HServerCliOpts();
      options.serverId = offset;
      options.port = hserverPort;
      options.gossipPort = hserverGossipPort;
      options.address = "localhost";
      options.metaHost = metaHost;
      hserverConfs.add(options);
      hserverInnerUrls.add("hserver" + offset + ":" + hserverGossipPort);
    }

    logger.info("hserverInnerUrls: {}", hserverInnerUrls);
    hservers.addAll(bootstrapHServerCluster(hserverConfs, hserverInnerUrls, dataDir));
    hservers.forEach(h -> h.waitingFor(Wait.forLogMessage(".*Cluster is ready!.*", 1)));
    hservers.forEach(h -> logger.info(h.getLogs()));
    var serverHosts =
        hservers.stream()
            .map(h -> h.getHost() + ":" + h.getFirstMappedPort())
            .collect(Collectors.joining(","));

    Object testInstance = context.getRequiredTestInstance();
    testInstance
        .getClass()
        .getMethod("setHStreamUrl", String.class)
        .invoke(testInstance, serverHosts);
  }

  @Override
  public void afterEach(ExtensionContext context) throws Exception {
    Thread.sleep(500);

    // waiting for servers to flush logs
    for (int i = 0; i < hservers.size(); i++) {
      var hserver = hservers.get(i);
      writeLog(context, "hserver-" + i, grp, hserver.getLogs());
      hserver.close();
    }

    hservers.clear();
    hserverUrls.clear();
    hserverInnerUrls.clear();
    count.set(0);
    writeLog(context, "hstore", grp, hstore.getLogs());
    hstore.close();
    writeLog(context, "zk", grp, zk.getLogs());
    zk.close();
    rq.close();

    logger.info("total time is = {}ms", System.currentTimeMillis() - beginTime);
    printEndFlag(context);
  }
}

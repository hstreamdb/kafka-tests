/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package kafka.server

import java.io._
import java.net.{InetAddress, ServerSocket, Socket}
import java.nio.file.{Path, Paths, StandardOpenOption}
import java.util.Properties
import org.apache.kafka.common.config.{
  AbstractConfig,
  ConfigDef,
  ConfigException,
  ConfigResource,
  SaslConfigs,
  SecurityConfig,
  SslClientAuth,
  SslConfigs,
  TopicConfig
}
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.network.ListenerName

import scala.collection.{mutable, Map, Seq}
import scala.annotation.nowarn
import scala.sys.process._
import org.yaml.snakeyaml.Yaml
import kafka.cluster.EndPoint
import kafka.utils.{CoreUtils, Logging}
import scala.jdk.CollectionConverters._
import kafka.utils.Implicits._
import scala.util.Random

// Copy from app/src/test/scala/utils/kafka/utils/TestUtils.scala
object TestUtils {
  def getUnusedPort(): Int = getUnusedPorts(0).head

  def getUnusedPorts(n: Int): Seq[Int] = {
    val sockPorts = (0 to n).map(_ => {
      // There is no need to be done in a try/finally
      val serverSocket = new ServerSocket(0)
      val port = serverSocket.getLocalPort()
      (serverSocket, port)
    })
    sockPorts.map {
      case (serverSocket, port) => {
        // close server
        serverSocket.close()
        // wait until the port is released
        waitUntilTrue(
          () => {
            try {
              val sock = new Socket("localhost", port)
              sock.close()
              false
            } catch {
              case _: Exception => true
            }
          },
          s"Port $port is still in use",
          waitTimeMs = 60000
        )
        port
      }
    }
  }

  def waitUntilTrue(
      condition: () => Boolean,
      msg: => String,
      waitTimeMs: Long = 60000L,
      pause: Long = 100L
  ): Unit = {
    val startTime = System.currentTimeMillis()
    while (true) {
      if (condition())
        return
      if (System.currentTimeMillis() > startTime + waitTimeMs)
        throw new RuntimeException(s"Timed out waiting for condition: $msg")
      Thread.sleep(waitTimeMs.min(pause))
    }

    // should never hit here
    throw new RuntimeException("unexpected error")
  }
}

object Defaults {

  /**
   * ********* Current hstream kafka config **********
   */
  val BrokerId = -1
  val Port = 9092
  val AdvertisedAddress = "127.0.0.1"
  val MetaStoreUri = "zk://127.0.0.1:2181"
  val GossipPort = 6571
  val StoreConfig = "/store/logdevice/logdevice.conf"
  val ListenerSecurityProtocolMap: String = EndPoint.DefaultSecurityProtocolMap
    .map { case (listenerName, securityProtocol) =>
      s"${listenerName.value}:${securityProtocol.name}"
    }
    .mkString(",")
  val AutoCreateTopicsEnable = true
  // Currently not supported in hstream
  val NumPartitions = 1
  val DefaultReplicationFactor = 1
  // KAFKA_TO_HSTREAM: kafka default value is 3
  val DefaultOffsetsTopicReplicationFactor: Short = 1
  val GroupInitialRebalanceDelayMs = 3000
  val FetchMaxBytes = 55 * 1024 * 1024

  // TODO: KAFKA_ORIGINAL
  // val Listeners = "PLAINTEXT://:9092"
}

object KafkaConfig {

  /**
   * ********* Current hstream kafka config **********
   */
  val BrokerIdProp = "broker.id"
  val PortProp = "port"
  val AdvertisedAddressProp = "advertised.address"
  val MetaStoreUriProp = "metastore.uri"
  val GossipPortProp = "gossip.port"
  val StoreConfigProp = "store.config"
  val AdvertisedListenersProp = "advertised.listeners"
  val ListenerSecurityProtocolMapProp = "listener.security.protocol.map"
  val AutoCreateTopicsEnableProp = "auto.create.topics.enable"
  val NumPartitionsProp = "num.partitions"
  val DefaultReplicationFactorProp = "default.replication.factor"
  val OffsetsTopicReplicationFactorProp = "offsets.topic.replication.factor"
  val GroupInitialRebalanceDelayMsProp = "group.initial.rebalance.delay.ms"
  val FetchMaxBytes = "fetch.max.bytes"

  // TODO: KAFKA_ORIGINAL
  // val ListenersProp = "listeners"
  val SaslKerberosServiceNameProp = SaslConfigs.SASL_KERBEROS_SERVICE_NAME

  @nowarn("cat=deprecation")
  val configDef = {
    import ConfigDef.Importance._
    import ConfigDef.Range._
    import ConfigDef.Type._
    import ConfigDef.ValidString._

    new ConfigDef()
      .define(BrokerIdProp, INT, Defaults.BrokerId, HIGH, "$BrokerIdDoc")
      .define(PortProp, INT, Defaults.Port, HIGH, "$PortDoc")
      .define(AdvertisedAddressProp, STRING, Defaults.AdvertisedAddress, HIGH, "$AdvertisedAddressDoc")
      .define(MetaStoreUriProp, STRING, Defaults.MetaStoreUri, HIGH, "$MetaStoreUriDoc")
      .define(GossipPortProp, INT, Defaults.GossipPort, HIGH, "$GossipPortDoc")
      .define(StoreConfigProp, STRING, Defaults.StoreConfig, HIGH, "$StoreConfigDoc")
      .define(AdvertisedListenersProp, STRING, null, HIGH, "$AdvertisedListenersDoc")
      .define(
        ListenerSecurityProtocolMapProp,
        STRING,
        Defaults.ListenerSecurityProtocolMap,
        LOW,
        "$ListenerSecurityProtocolMapDoc"
      )
      .define(
        AutoCreateTopicsEnableProp,
        BOOLEAN,
        Defaults.AutoCreateTopicsEnable,
        HIGH,
        "AutoCreateTopicsEnableDoc"
      )
      .define(NumPartitionsProp, INT, Defaults.NumPartitions, atLeast(1), MEDIUM, "$NumPartitionsDoc")
      .define(
        DefaultReplicationFactorProp,
        INT,
        Defaults.DefaultReplicationFactor,
        MEDIUM,
        "$DefaultReplicationFactorDoc"
      )
      .define(
        OffsetsTopicReplicationFactorProp,
        SHORT,
        Defaults.DefaultOffsetsTopicReplicationFactor,
        atLeast(1),
        HIGH,
        "$OffsetsTopicReplicationFactorDoc"
      )
      .define(
        GroupInitialRebalanceDelayMsProp,
        INT,
        Defaults.GroupInitialRebalanceDelayMs,
        MEDIUM,
        "$GroupInitialRebalanceDelayMsDoc"
      )
      .define(SaslKerberosServiceNameProp, STRING, null, MEDIUM, "$SaslKerberosServiceNameDoc")

      /**
       * ********* Fetch Configuration *************
       */
      .define(FetchMaxBytes, INT, Defaults.FetchMaxBytes, atLeast(1024), MEDIUM, "$FetchMaxBytesDoc")

    // TODO: KAFKA_ORIGINAL
    // .define(ListenersProp, STRING, Defaults.Listeners, HIGH, "* ListenersDoc *")
  }

  /**
   * Copy a configuration map, populating some keys that we want to treat as synonyms.
   */
  // KAFKA_ORIGINAL
  // Doesn't need to be done in hstream
  // def populateSynonyms(input: util.Map[_, _]): util.Map[Any, Any] = {
  //   val output = new util.HashMap[Any, Any](input)
  //   val brokerId = output.get(KafkaConfig.BrokerIdProp)
  //   val nodeId = output.get(KafkaConfig.NodeIdProp)
  //   if (brokerId == null && nodeId != null) {
  //     output.put(KafkaConfig.BrokerIdProp, nodeId)
  //   } else if (brokerId != null && nodeId == null) {
  //     output.put(KafkaConfig.NodeIdProp, brokerId)
  //   }
  //   output
  // }

  // KAFKA_TO_HSTREAM
  def rePropsTesting(props: Properties): (Properties, Map[String, Object]) = {
    val testingConfig = {
      val x = props.remove("testing").asInstanceOf[Map[String, Object]]
      if (x == null) Map[String, Object]()
      else x
    }
    (props, testingConfig)
  }

  // KAFKA_TO_HSTREAM
  def fromProps(props: Properties, doLog: Boolean): KafkaConfig = {
    new KafkaConfig(doLog, rePropsTesting(props))
  }

  def fromProps(props: Properties): KafkaConfig =
    fromProps(props, true)

  def fromProps(defaults: Properties, overrides: Properties): KafkaConfig =
    fromProps(defaults, overrides, true)

  def fromProps(defaults: Properties, overrides: Properties, doLog: Boolean): KafkaConfig = {
    val props = new Properties()
    props ++= defaults
    props ++= overrides
    fromProps(props, doLog)
  }

  // TODO: KAFKA_ORIGINAL
  // def apply(props: java.util.Map[_, _], doLog: Boolean = true): KafkaConfig = new KafkaConfig(props, doLog)
}

class KafkaConfig private (
    doLog: Boolean,
    val props: java.util.Map[_, _],
    val testingConfig: Map[String, Object] = Map() // Only used for this testing
) extends AbstractConfig(KafkaConfig.configDef, props, doLog)
    with Logging {

  // KAFKA_TO_HSTREAM
  def this(doLog: Boolean, props: (java.util.Map[_, _], Map[String, Object])) =
    this(doLog, props._1, props._2)

  // KAFKA_TO_HSTREAM
  def this(props: Properties) = this(true, KafkaConfig.rePropsTesting(props))

  // KAFKA_TO_HSTREAM
  def this(props: Properties, doLog: Boolean) = this(doLog, KafkaConfig.rePropsTesting(props))

  val port = getInt(KafkaConfig.PortProp)
  val advertisedAddress = getString(KafkaConfig.AdvertisedAddressProp)
  val brokerId = getInt(KafkaConfig.BrokerIdProp)
  val numPartitions = getInt(KafkaConfig.NumPartitionsProp)
  val defaultReplicationFactor: Int = getInt(KafkaConfig.DefaultReplicationFactorProp)
  val fetchMaxBytes = getInt(KafkaConfig.FetchMaxBytes)

  def hstreamKafkaBrokerProperties: Map[Any, Any] = {
    val properties = new mutable.HashMap[Any, Any]()
    properties.put(KafkaConfig.NumPartitionsProp, getInt(KafkaConfig.NumPartitionsProp))
    properties.put(KafkaConfig.DefaultReplicationFactorProp, getInt(KafkaConfig.DefaultReplicationFactorProp))
    properties.put(KafkaConfig.AutoCreateTopicsEnableProp, getBoolean(KafkaConfig.AutoCreateTopicsEnableProp))
    properties.put(
      KafkaConfig.OffsetsTopicReplicationFactorProp,
      getShort(KafkaConfig.OffsetsTopicReplicationFactorProp)
    )
    if (!props.containsKey(KafkaConfig.GroupInitialRebalanceDelayMsProp))
      properties.put(KafkaConfig.GroupInitialRebalanceDelayMsProp, "0")
    else
      properties.put(KafkaConfig.GroupInitialRebalanceDelayMsProp, getInt(KafkaConfig.GroupInitialRebalanceDelayMsProp))
    properties.put(KafkaConfig.FetchMaxBytes, fetchMaxBytes)
    info("Start HStream Kafka Broker With Properties: ")
    properties.foreach { case (k, v) => info(s"$k = $v") }
    properties
  }

  // Use advertised listeners if defined, fallback to listeners otherwise
  def effectiveAdvertisedListeners: Seq[EndPoint] = {
    val advertisedListenersProp = getString(KafkaConfig.AdvertisedListenersProp)
    if (advertisedListenersProp != null)
      CoreUtils.listenerListToEndPoints(
        advertisedListenersProp,
        effectiveListenerSecurityProtocolMap,
        requireDistinctPorts = false
      )
    else {
      // TODO KAFKA_ORIGINAL
      // listeners.filterNot(l => controllerListenerNames.contains(l.listenerName.value()))
      Seq(
        EndPoint(
          getString(KafkaConfig.AdvertisedAddressProp),
          getInt(KafkaConfig.PortProp),
          new ListenerName("PLAINTEXT"),
          SecurityProtocol.PLAINTEXT
        )
      )
    }
  }

  // TODO KAFKA_ORIGINAL
  // def listeners: Seq[EndPoint] =
  //  CoreUtils.listenerListToEndPoints(getString(KafkaConfig.ListenersProp), effectiveListenerSecurityProtocolMap)

  def effectiveListenerSecurityProtocolMap: Map[ListenerName, SecurityProtocol] = {
    val mapValue =
      getMap(KafkaConfig.ListenerSecurityProtocolMapProp, getString(KafkaConfig.ListenerSecurityProtocolMapProp))
        .map { case (listenerName, protocolName) =>
          ListenerName.normalised(listenerName) -> getSecurityProtocol(
            protocolName,
            KafkaConfig.ListenerSecurityProtocolMapProp
          )
        }
    // TODO KAFKA_ORIGINAL
    //   if (usesSelfManagedQuorum && !originals.containsKey(ListenerSecurityProtocolMapProp)) {
    //     // Nothing was specified explicitly for listener.security.protocol.map, so we are using the default value,
    //     // and we are using KRaft.
    //     // Add PLAINTEXT mappings for controller listeners as long as there is no SSL or SASL_{PLAINTEXT,SSL} in use
    //     def isSslOrSasl(name: String): Boolean = name.equals(SecurityProtocol.SSL.name) || name.equals(SecurityProtocol.SASL_SSL.name) || name.equals(SecurityProtocol.SASL_PLAINTEXT.name)
    //     // check controller listener names (they won't appear in listeners when process.roles=broker)
    //     // as well as listeners for occurrences of SSL or SASL_*
    //     if (controllerListenerNames.exists(isSslOrSasl) ||
    //       parseCsvList(getString(KafkaConfig.ListenersProp)).exists(listenerValue => isSslOrSasl(EndPoint.parseListenerName(listenerValue)))) {
    //       mapValue // don't add default mappings since we found something that is SSL or SASL_*
    //     } else {
    //       // add the PLAINTEXT mappings for all controller listener names that are not explicitly PLAINTEXT
    //       mapValue ++ controllerListenerNames.filterNot(SecurityProtocol.PLAINTEXT.name.equals(_)).map(
    //         new ListenerName(_) -> SecurityProtocol.PLAINTEXT)
    //     }
    //   } else {
    //     mapValue
    //   }
    mapValue
  }

  // TODO KAFKA_ORIGINAL
  // def controllerListenerNames: Seq[String] = {
  //   val value = Option(getString(KafkaConfig.ControllerListenerNamesProp)).getOrElse("")
  //   if (value.isEmpty) {
  //     Seq.empty
  //   } else {
  //     value.split(",")
  //   }
  // }

  private def getSecurityProtocol(protocolName: String, configName: String): SecurityProtocol = {
    try SecurityProtocol.forName(protocolName)
    catch {
      case _: IllegalArgumentException =>
        throw new ConfigException(s"Invalid security protocol `$protocolName` defined in $configName")
    }
  }

  private def getMap(propName: String, propValue: String): Map[String, String] = {
    try {
      CoreUtils.parseCsvMap(propValue)
    } catch {
      case e: Exception =>
        throw new IllegalArgumentException(
          "Error parsing configuration property '%s': %s".format(propName, e.getMessage)
        )
    }
  }

}

object TestConfig {

  val currentTestTimeMillis = System.currentTimeMillis()

  def parseConfigFile(
      configFile: String,
      startingIdNumber: Int,
      endingIdNumber: Int,
      testName: String,
      unusedPorts: Seq[Int]
  ): Seq[Properties] = {
    val configs = readConfigFile(configFile)
    val use = configs
      .getOrElse("use", throw new IllegalArgumentException("use is required in config file"))
      .asInstanceOf[String]
    if (use == "broker_container") {
      // Start hserver by docker
      val brokerContainer = configs
        .getOrElse(
          "broker_container",
          throw new IllegalArgumentException("broker_container is required in config file")
        )
        .asInstanceOf[java.util.Map[String, Object]]
      return createBrokerConfigsFromBrokerContainer(
        brokerContainer,
        startingIdNumber,
        endingIdNumber,
        testName,
        unusedPorts
      )
    } else if (use == "broker_connections") {
      // Directly connect to brokers
      val brokerConnections =
        configs
          .getOrElse(
            "broker_connections",
            throw new IllegalArgumentException("broker_connections is required in config file")
          )
          .asInstanceOf[java.util.List[java.util.Map[String, Object]]]
      brokerConnections.asScala.map(_.asScala.toMap).map { config =>
        val props = new Properties
        config.foreach { case (k, v) => props.put(k, v.toString) }
        props
      }
    } else {
      throw new IllegalArgumentException("use must be one of [broker_container, broker_connections]")
    }
  }

  private def createBrokerConfigsFromBrokerContainer(
      brokerContainer: java.util.Map[String, Object],
      startingIdNumber: Int,
      endingIdNumber: Int,
      testName: String,
      unusedPorts: Seq[Int]
  ): Seq[Properties] = {
    val brokerConfig = brokerContainer.get("config").asInstanceOf[java.util.Map[String, Object]].asScala
    val testingConfig: collection.mutable.Map[String, Object] =
      brokerContainer
        .get("testing_config")
        .asInstanceOf[java.util.Map[String, Object]]
        .asScala
    val spec = testingConfig
      .getOrElse("spec", throw new IllegalArgumentException("spec is required in testing_config"))
      .asInstanceOf[Int]

    if (spec == 1) {
      parseTestingConfig1(brokerConfig, testingConfig, startingIdNumber, endingIdNumber, testName, unusedPorts);
    } else if (spec == 2) {
      parseTestingConfig2(brokerConfig, testingConfig, startingIdNumber, endingIdNumber, testName, unusedPorts);
    } else if (spec == 3) {
      parseTestingConfig3(brokerConfig, testingConfig, startingIdNumber, endingIdNumber, testName, unusedPorts);
    } else {
      throw new IllegalArgumentException("Invalid testing spec!")
    }
  }

  // === spec 1: hstream
  private def parseTestingConfig1(
      brokerConfig: collection.mutable.Map[String, Object],
      testingConfig: collection.mutable.Map[String, Object],
      startingIdNumber: Int,
      endingIdNumber: Int,
      testName: String,
      unusedPorts: Seq[Int]
  ): Seq[Properties] = {
    val advertisedAddress = brokerConfig
      .getOrElse(
        "advertised.address",
        throw new IllegalArgumentException("advertised.address is required in broker_container")
      )
      .asInstanceOf[String]
    val basePort = testingConfig.get("base_port").asInstanceOf[Option[Int]]
    val initMode = testingConfig.getOrElse("init_mode", "boot").asInstanceOf[String]
    val commandTmpl = testingConfig
      .getOrElse(
        "command",
        throw new IllegalArgumentException("command is required in testing_config")
      )
      .asInstanceOf[String]
    // TODO: support rqlite
    val metastorePort = testingConfig
      .getOrElse("metastore_port", throw new IllegalArgumentException("metastore_port is required in testing_config"))
      .asInstanceOf[Int]
    val cleanContainerLog = testingConfig.getOrElse("container_logs_clean", false).asInstanceOf[Boolean]

    var seedNodes = ""

    // generate
    val props = (startingIdNumber to endingIdNumber).zipWithIndex.map { case (nodeId, idx) =>
      val prop = new Properties
      val port = basePort match {
        case None    => unusedPorts(idx * 2)
        case Some(p) => p + idx * 2
      }
      val gossipPort = basePort match {
        case None    => unusedPorts(idx * 2 + 1)
        case Some(p) => p + idx * 2 + 1
      }
      if (initMode == "join") {
        // we use a fixed port (the first gossipPort) for seed node when initMode is "join"
        if (idx == 0) {
          seedNodes = s"127.0.0.1:$gossipPort"
        }
      } else if (initMode == "boot") {
        val sepr = if (idx == 0) "" else ","
        seedNodes += s"${sepr}127.0.0.1:$gossipPort"
      } else {
        throw new IllegalArgumentException("init_mode must in [\"boot\", \"join\"]")
      }
      // broker config
      prop.put("broker.id", nodeId.toString)
      prop.put("port", port.toString)
      prop.put("gossip.port", gossipPort.toString)
      brokerConfig.foreach { case (k, v) => prop.put(k, v.toString) }
      // testing config
      val args = s"""--server-id $nodeId
                --port $port --gossip-port $gossipPort
                --advertised-address $advertisedAddress
                --metastore-uri zk://127.0.0.1:$metastorePort
                --store-config /data/store/logdevice.conf
                """.stripMargin.linesIterator.mkString(" ").trim
      (prop, args)
    }
    // The seedNodes is determined after all broker configs are created
    return props.map {
      case (prop, args) => {
        val newArgs = s"$args --seed-nodes $seedNodes"
        var newTestingConfig = testingConfig.clone()
        newTestingConfig.update("command", commandTmpl.format(newArgs))
        newTestingConfig.put("container_logs_dir", getContainerLogsDir(testName, cleanContainerLog))
        prop.put("testing", newTestingConfig)
        prop
      }
    }
  }

  // === spec 2: hornbill
  private def parseTestingConfig2(
      brokerConfig: collection.mutable.Map[String, Object],
      testingConfig: collection.mutable.Map[String, Object],
      startingIdNumber: Int,
      endingIdNumber: Int,
      testName: String,
      unusedPorts: Seq[Int]
  ): Seq[Properties] = {
    val advertisedAddress = brokerConfig
      .getOrElse(
        "advertised.address",
        throw new IllegalArgumentException("advertised.address is required in broker_container")
      )
      .asInstanceOf[String]
    val basePort = testingConfig.get("base_port").asInstanceOf[Option[Int]]
    val configMetaServerPort = testingConfig.get("meta_port").asInstanceOf[Option[Int]]
    val commandTmpl = testingConfig
      .getOrElse(
        "command",
        throw new IllegalArgumentException("command is required in testing_config")
      )
      .asInstanceOf[String]
    val cleanContainerLog = testingConfig.getOrElse("container_logs_clean", false).asInstanceOf[Boolean]
    val storeConfig = testingConfig.getOrElse(
      "store_config",
      throw new IllegalArgumentException("store_config is required in testing_config")
    )

    // For unusedPorts, diff with parseTestingConfig1:
    //
    // 1. Choose 0, 2, 4, ... as server port
    // 2. Choose 1 as meta server port

    // XXX: Start only one meta server
    val metaServerPort = configMetaServerPort match {
      case None    => unusedPorts(1) // Choose 1 as meta server port
      case Some(p) => p
    }
    val metaServerContainerName: String = {
      val rand = Random.alphanumeric.take(10).mkString
      s"kafka-tests-metaserver-${scala.util.Properties.userName}-$rand"
    }

    // generate
    val props = (startingIdNumber to endingIdNumber).zipWithIndex.map { case (nodeId, idx) =>
      val prop = new Properties
      // Choose 0, 2, 4, ... as server port
      val port = basePort match {
        case None    => unusedPorts(idx * 2)
        case Some(p) => p + idx * 2
      }
      // broker config
      prop.put("broker.id", nodeId.toString)
      prop.put("port", port.toString)
      // prop.put("gossip.port", "0")
      brokerConfig.foreach { case (k, v) => prop.put(k, v.toString) }
      // testing config
      val args = s"""--server-id $nodeId
                --listeners plaintext://0.0.0.0:$port
                --advertised-listeners plaintext://$advertisedAddress:$port
                --meta-servers http://127.0.0.1:$metaServerPort
                --store-config $storeConfig
                """.stripMargin.linesIterator.mkString(" ").trim
      (prop, args)
    }
    // The seedNodes is determined after all broker configs are created
    return props.map {
      case (prop, args) => {
        var newTestingConfig = testingConfig.clone()
        newTestingConfig.update("command", commandTmpl.format(args))
        newTestingConfig.put("container_logs_dir", getContainerLogsDir(testName, cleanContainerLog))
        newTestingConfig.put("metaserver_port", metaServerPort.asInstanceOf[Object])
        newTestingConfig.put("metaserver_container_name", metaServerContainerName)
        prop.put("testing", newTestingConfig)
        prop
      }
    }
  }

  // === spec 3: flowmq
  private def parseTestingConfig3(
      brokerConfig: collection.mutable.Map[String, Object],
      testingConfig: collection.mutable.Map[String, Object],
      startingIdNumber: Int,
      endingIdNumber: Int,
      testName: String,
      unusedPorts: Seq[Int]
  ): Seq[Properties] = {
    val advertisedAddress = brokerConfig
      .getOrElse(
        "advertised.address",
        throw new IllegalArgumentException("advertised.address is required in broker_container")
      )
      .asInstanceOf[String]
    val basePort = testingConfig.get("base_port").asInstanceOf[Option[Int]]
    val commandTmpl = testingConfig
      .getOrElse(
        "command",
        throw new IllegalArgumentException("command is required in testing_config")
      )
      .asInstanceOf[String]
    val cleanContainerLog = testingConfig.getOrElse("container_logs_clean", false).asInstanceOf[Boolean]
    val storeConfig = testingConfig.getOrElse(
      "store_config",
      throw new IllegalArgumentException("store_config is required in testing_config")
    )

    // generate
    val props = (startingIdNumber to endingIdNumber).zipWithIndex.map { case (nodeId, idx) =>
      val prop = new Properties

      // For unusedPorts,
      //
      // 1. Choose 0, 2, 4, ... as kafka port
      // 2. Choose 1, 3, 5, ... as mqtt port
      val port = basePort match {
        case None    => unusedPorts(idx * 2)
        case Some(p) => p + idx * 2
      }
      val mqttPort = basePort match {
        case None    => unusedPorts(idx * 2 + 1)
        case Some(p) => p + idx * 2 + 1
      }
      val httpPort = TestUtils.getUnusedPort();

      // broker config
      prop.put("broker.id", nodeId.toString)
      prop.put("port", port.toString)
      // prop.put("gossip.port", "0")
      brokerConfig.foreach { case (k, v) => prop.put(k, v.toString) }
      // testing config
      val args = s"""--port $mqttPort --http-api-port $httpPort
                --with-kafka $port --kafka-advertised-address $advertisedAddress
                --cluster-file $storeConfig
                """.stripMargin.linesIterator.mkString(" ").trim
      (prop, args)
    }
    // The seedNodes is determined after all broker configs are created
    return props.map {
      case (prop, args) => {
        var newTestingConfig = testingConfig.clone()
        newTestingConfig.update("command", commandTmpl.format(args))
        newTestingConfig.put("container_logs_dir", getContainerLogsDir(testName, cleanContainerLog))
        prop.put("testing", newTestingConfig)
        prop
      }
    }
  }

  private def readConfigFile(configFile: String): Map[String, Object] = {
    val inputStream = new FileInputStream(configFile)
    try {
      val yaml = new Yaml()
      yaml
        .load(new InputStreamReader(inputStream, "UTF-8"))
        .asInstanceOf[java.util.Map[String, Object]]
        .asScala
    } catch {
      case e: Throwable => throw e
    } finally {
      inputStream.close()
    }
  }

  private def formatTestNameAsFile(testName: String) = {
    testName.replaceAll("""\(|\)|\s|\[|\]|=""", "_").replaceAll("(^_*)|(_*$)", "")
  }

  private def getContainerLogsDir(testName: String, cleanBefore: Boolean = false): Path = {
    val testFilename = formatTestNameAsFile(testName)
    val proj = sys.props.get("user.dir").getOrElse(".")
    val containerLogsDir = s"$proj/build/reports/logs/$testFilename-$currentTestTimeMillis"
    // FIXME: how about moving clean function into "QuorumTestHarness",
    // Which means, in QuorumTestHarness, do "rm -rf $proj/build/reports/logs" once.
    if (cleanBefore) {
      // TODO: Safer way
      s"bash -c \"rm -rf $proj/build/reports/logs/$testFilename-*\"".!
    }
    Paths.get(containerLogsDir)
  }

}

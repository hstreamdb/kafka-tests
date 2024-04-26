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

import java.util.Properties
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef, ConfigException, ConfigResource, SaslConfigs, SecurityConfig, SslClientAuth, SslConfigs, TopicConfig}
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.network.ListenerName

import scala.collection.{Map, Seq, immutable, mutable}
import scala.annotation.nowarn
import kafka.cluster.EndPoint
import kafka.utils.{CoreUtils, Logging}
import kafka.utils.Implicits._

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
      .define(OffsetsTopicReplicationFactorProp, SHORT, Defaults.DefaultOffsetsTopicReplicationFactor, atLeast(1), HIGH, "$OffsetsTopicReplicationFactorDoc")
      .define(SaslKerberosServiceNameProp, STRING, null, MEDIUM, "$SaslKerberosServiceNameDoc")

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

  def hstreamKafkaBrokerProperties: Map[Any, Any] = {
    val props = new mutable.HashMap[Any, Any]()
    props.put(KafkaConfig.NumPartitionsProp, getInt(KafkaConfig.NumPartitionsProp))
    props.put(KafkaConfig.DefaultReplicationFactorProp, getInt(KafkaConfig.DefaultReplicationFactorProp))
    props.put(KafkaConfig.AutoCreateTopicsEnableProp, getBoolean(KafkaConfig.AutoCreateTopicsEnableProp))
    props.put(KafkaConfig.OffsetsTopicReplicationFactorProp, getShort(KafkaConfig.OffsetsTopicReplicationFactorProp))
    props
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

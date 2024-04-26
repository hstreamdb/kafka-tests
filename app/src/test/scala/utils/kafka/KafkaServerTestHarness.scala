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

// From: scala/unit/kafka/integration/KafkaServerTestHarness.scala
package kafka.integration

import kafka.network.SocketServer
import kafka.server._
import kafka.utils.TestUtils.{createAdminClient, resource}
import kafka.utils.{NotNothing, TestUtils}
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests._
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.{Time, Utils}
import org.apache.kafka.common.{KafkaException, Uuid}
import org.apache.kafka.metadata.BrokerState
import org.junit.jupiter.api.{AfterEach, BeforeEach, TestInfo}

import java.io.{DataInputStream, DataOutputStream, File}
import java.net.Socket
import java.nio.ByteBuffer
import java.util.{Arrays, Properties}
import scala.annotation.nowarn
import scala.collection.{Seq, mutable}
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
// import org.apache.kafka.controller.ControllerRequestContextUtil.ANONYMOUS_CONTEXT

/**
 * A test harness that brings up some number of broker nodes
 */
abstract class KafkaServerTestHarness extends QuorumTestHarness {
  var instanceConfigs: Seq[KafkaConfig] = _

  private val _brokers = new mutable.ArrayBuffer[KafkaBroker]

  /**
   * Get the list of brokers, which could be either BrokerServer objects or KafkaServer objects.
   */
  def brokers: mutable.Buffer[KafkaBroker] = _brokers

  /**
   * Get the list of brokers, as instances of KafkaServer. This method should only be used when dealing with brokers
   * that use ZooKeeper.
   */
  // def servers: mutable.Buffer[KafkaServer] = {
  //    checkIsZKTest()
  //   _brokers.asInstanceOf[mutable.Buffer[KafkaServer]]
  // }
  // For hstream
  def servers: mutable.Buffer[KafkaBroker] = _brokers

  var alive: Array[Boolean] = _

  /**
   * Implementations must override this method to return a set of KafkaConfigs. This method will be invoked for every
   * test and should not reuse previous configurations unless they select their ports randomly when servers are started.
   */
  def generateConfigs: Seq[KafkaConfig]

  /**
   * Override this in case ACLs or security credentials must be set before `servers` are started.
   *
   * This is required in some cases because of the topic creation in the setup of `IntegrationTestHarness`. If the ACLs
   * are only set later, tests may fail. The failure could manifest itself as a cluster action authorization exception
   * when processing an update metadata request (controller -> broker) or in more obscure ways (e.g. __consumer_offsets
   * topic replication fails because the metadata cache has no brokers as a previous update metadata request failed due
   * to an authorization exception).
   *
   * The default implementation of this method is a no-op.
   */
  def configureSecurityBeforeServersStart(testInfo: TestInfo): Unit = {}

  /**
   * Override this in case Tokens or security credentials needs to be created after `servers` are started. The default
   * implementation of this method is a no-op.
   */
  def configureSecurityAfterServersStart(): Unit = {}

  def configs: Seq[KafkaConfig] = {
    if (instanceConfigs == null)
      instanceConfigs = generateConfigs
    instanceConfigs
  }

//   def serverForId(id: Int): Option[KafkaServer] = servers.find(s => s.config.brokerId == id)

//   def boundPort(server: KafkaServer): Int = server.boundPort(listenerName)

  def bootstrapServers(listenerName: ListenerName = listenerName): String = {
    TestUtils.bootstrapServers(_brokers, listenerName)
  }

  protected def securityProtocol: SecurityProtocol = SecurityProtocol.PLAINTEXT
  protected def listenerName: ListenerName = ListenerName.forSecurityProtocol(securityProtocol)
  protected def trustStoreFile: Option[File] = None
  protected def serverSaslProperties: Option[Properties] = None
  protected def clientSaslProperties: Option[Properties] = None
  protected def brokerTime(brokerId: Int): Time = Time.SYSTEM

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.setUp(testInfo)

    if (configs.isEmpty)
      throw new KafkaException("Must supply at least one server config.")

    // default implementation is a no-op, it is overridden by subclasses if required
    configureSecurityBeforeServersStart(testInfo)

    createBrokers(startup = true)

    // default implementation is a no-op, it is overridden by subclasses if required
    configureSecurityAfterServersStart()
  }

  @AfterEach
  override def tearDown(): Unit = {
    TestUtils.shutdownServers(_brokers)
    super.tearDown()
  }

//   def recreateBrokers(reconfigure: Boolean = false, startup: Boolean = false): Unit = {
//     // The test must be allowed to fail and be torn down if an exception is raised here.
//     if (reconfigure) {
//       instanceConfigs = null
//     }
//     if (configs.isEmpty)
//       throw new KafkaException("Must supply at least one server config.")
//
//     TestUtils.shutdownServers(_brokers, deleteLogDirs = false)
//     _brokers.clear()
//     Arrays.fill(alive, false)
//
//     createBrokers(startup)
//   }
//
//   def createOffsetsTopic(
//     listenerName: ListenerName = listenerName,
//     adminClientConfig: Properties = new Properties
//   ): Unit = {
//     if (isKRaftTest()) {
//       resource(createAdminClient(brokers, listenerName, adminClientConfig)) { admin =>
//         TestUtils.createOffsetsTopicWithAdmin(admin, brokers)
//       }
//     } else {
//       TestUtils.createOffsetsTopic(zkClient, servers)
//     }
//   }

  /**
   * Create a topic. Wait until the leader is elected and the metadata is propagated to all brokers. Return the leader
   * for each partition.
   */
  // def createTopic(
  //   topic: String,
  //   numPartitions: Int = 1,
  //   replicationFactor: Int = 1,
  //   topicConfig: Properties = new Properties,
  //   listenerName: ListenerName = listenerName,
  //   adminClientConfig: Properties = new Properties
  // ): scala.collection.immutable.Map[Int, Int] = {
  //   if (isKRaftTest()) {
  //     resource(createAdminClient(brokers, listenerName, adminClientConfig)) { admin =>
  //       TestUtils.createTopicWithAdmin(
  //         admin = admin,
  //         topic = topic,
  //         brokers = brokers,
  //         numPartitions = numPartitions,
  //         replicationFactor = replicationFactor,
  //         topicConfig = topicConfig
  //       )
  //     }
  //   } else {
  //     TestUtils.createTopic(
  //       zkClient = zkClient,
  //       topic = topic,
  //       numPartitions = numPartitions,
  //       replicationFactor = replicationFactor,
  //       servers = servers,
  //       topicConfig = topicConfig
  //     )
  //   }
  // }
  def createTopic(
      topic: String,
      numPartitions: Int = 1,
      replicationFactor: Int = 1,
      topicConfig: Properties = new Properties,
      listenerName: ListenerName = listenerName,
      adminClientConfig: Properties = new Properties
  ): scala.collection.immutable.Map[Int, Int] = {
    resource(createAdminClient(brokers, listenerName, adminClientConfig)) { admin =>
      TestUtils.createTopicWithAdmin(
        admin = admin,
        topic = topic,
        brokers = brokers,
        numPartitions = numPartitions,
        replicationFactor = replicationFactor,
        topicConfig = topicConfig
      )
    }
  }

//   /**
//    * Create a topic in ZooKeeper using a customized replica assignment.
//    * Wait until the leader is elected and the metadata is propagated to all brokers.
//    * Return the leader for each partition.
//    */
//   def createTopicWithAssignment(
//     topic: String,
//     partitionReplicaAssignment: collection.Map[Int, Seq[Int]],
//     listenerName: ListenerName = listenerName
//   ): scala.collection.immutable.Map[Int, Int] =
//     if (isKRaftTest()) {
//       resource(createAdminClient(brokers, listenerName)) { admin =>
//         TestUtils.createTopicWithAdmin(
//           admin = admin,
//           topic = topic,
//           replicaAssignment = partitionReplicaAssignment,
//           brokers = brokers
//         )
//       }
//     } else {
//       TestUtils.createTopic(
//         zkClient,
//         topic,
//         partitionReplicaAssignment,
//         servers
//       )
//     }

  // KAFKA_TO_HSTREAM
  //
  // def deleteTopic(
  //   topic: String,
  //   listenerName: ListenerName = listenerName
  // ): Unit = {
  //   if (isKRaftTest()) {
  //     resource(createAdminClient(brokers, listenerName)) { admin =>
  //       TestUtils.deleteTopicWithAdmin(
  //         admin = admin,
  //         topic = topic,
  //         brokers = aliveBrokers)
  //     }
  //   } else {
  //     adminZkClient.deleteTopic(topic)
  //   }
  // }
  def deleteTopic(
      topic: String,
      listenerName: ListenerName = listenerName
  ): Unit = {
    resource(createAdminClient(brokers, listenerName)) { admin =>
      TestUtils.deleteTopicWithAdmin(admin = admin, topic = topic, brokers = aliveBrokers)
    }
  }

//   def addAndVerifyAcls(acls: Set[AccessControlEntry], resource: ResourcePattern): Unit = {
//     TestUtils.addAndVerifyAcls(brokers, acls, resource, controllerServers)
//   }
//
//   def removeAndVerifyAcls(acls: Set[AccessControlEntry], resource: ResourcePattern): Unit = {
//     TestUtils.removeAndVerifyAcls(brokers, acls, resource, controllerServers)
//   }

  /**
   * Pick a broker at random and kill it if it isn't already dead Return the id of the broker killed
   */
  def killRandomBroker(): Int = {
    val index = TestUtils.random.nextInt(_brokers.length)
    killBroker(index)
    index
  }

  def killBroker(index: Int): Unit = {
    if (alive(index)) {
      _brokers(index).shutdown()
      _brokers(index).awaitShutdown()
      alive(index) = false
    }
  }

  /**
   * Restart any dead brokers
   */
  def restartDeadBrokers(reconfigure: Boolean = false): Unit = {
    if (reconfigure) {
      instanceConfigs = null
    }
    if (configs.isEmpty)
      throw new KafkaException("Must supply at least one server config.")
    for (i <- _brokers.indices if !alive(i)) {
      if (reconfigure) {
        _brokers(i) = createBrokerFromConfig(configs(i))
      }
      info("Restart broker %d".format(i))
      _brokers(i).startup()
      alive(i) = true
    }
  }

//   def waitForUserScramCredentialToAppearOnAllBrokers(clientPrincipal: String, mechanismName: String): Unit = {
//     _brokers.foreach { server =>
//       val cache = server.credentialProvider.credentialCache.cache(mechanismName, classOf[ScramCredential])
//       TestUtils.waitUntilTrue(() => cache.get(clientPrincipal) != null, s"SCRAM credentials not created for $clientPrincipal")
//     }
//   }
//
//   def getController(): KafkaServer = {
//     checkIsZKTest()
//     val controllerId = TestUtils.waitUntilControllerElected(zkClient)
//     servers.filter(s => s.config.brokerId == controllerId).head
//   }
//

  // KAFKA_TO_HSTREAM: move from: BaseRequestTest.scala
  private var correlationId = 0

  // KAFKA_TO_HSTREAM: move from: BaseRequestTest.scala
  def anySocketServer: SocketServer = {
    brokers
      .find { broker =>
        val state = broker.brokerState
        state != BrokerState.NOT_RUNNING && state != BrokerState.SHUTTING_DOWN
      }
      .map(_.socketServer)
      .getOrElse(throw new IllegalStateException("No live broker is available"))
  }

  // KAFKA_TO_HSTREAM: move from: BaseRequestTest.scala
  private def apiVersions: ApiVersionsResponse = {
    val request = new ApiVersionsRequest.Builder().build()
    connectAndReceive[ApiVersionsResponse](request, anySocketServer)
  }

  // KAFKA_TO_HSTREAM: move from: BaseRequestTest.scala
  def findProperApiVersion(apiKey: ApiKeys): Short = {
    val version = apiVersions.apiVersion(apiKey.id)
    if (version == null) throw new IllegalArgumentException(s"API key $apiKey is not supported by the broker")
    apiKey.latestVersion().min(version.maxVersion)
  }

  def findMinApiVersion(apiKey: ApiKeys): Short = {
    val version = apiVersions.apiVersion(apiKey.id)
    if (version == null) throw new IllegalArgumentException(s"API key $apiKey is not supported by the broker")
    apiKey.oldestVersion().max(version.minVersion)
  }

  // Move from: BaseRequestTest.scala
  def connect(socketServer: SocketServer = anySocketServer, listenerName: ListenerName = listenerName): Socket = {
    new Socket("localhost", socketServer.boundPort(listenerName))
  }

  // KAFKA_TO_HSTREAM: move from: BaseRequestTest.scala
  private def sendRequest(socket: Socket, request: Array[Byte]): Unit = {
    val outgoing = new DataOutputStream(socket.getOutputStream)
    outgoing.writeInt(request.length)
    outgoing.write(request)
    outgoing.flush()
  }

  // KAFKA_TO_HSTREAM: move from: BaseRequestTest.scala
  def receive[T <: AbstractResponse](socket: Socket, apiKey: ApiKeys, version: Short)(implicit
                                                                                      classTag: ClassTag[T],
                                                                                      @nowarn("cat=unused") nn: NotNothing[T]
  ): T = {
    val incoming = new DataInputStream(socket.getInputStream)
    val len = incoming.readInt()

    val responseBytes = new Array[Byte](len)
    incoming.readFully(responseBytes)

    val responseBuffer = ByteBuffer.wrap(responseBytes)
    ResponseHeader.parse(responseBuffer, apiKey.responseHeaderVersion(version))

    AbstractResponse.parseResponse(apiKey, responseBuffer, version) match {
      case response: T => response
      case response =>
        throw new ClassCastException(
          s"Expected response with type ${classTag.runtimeClass}, but found ${response.getClass}"
        )
    }
  }

  // KAFKA_TO_HSTREAM: move from: BaseRequestTest.scala
  def sendAndReceive[T <: AbstractResponse](
                                             request: AbstractRequest,
                                             socket: Socket,
                                             clientId: String = "client-id",
                                             correlationId: Option[Int] = None
                                           )(implicit classTag: ClassTag[T], nn: NotNothing[T]): T = {
    send(request, socket, clientId, correlationId)
    receive[T](socket, request.apiKey, request.version)
  }

  // KAFKA_TO_HSTREAM: move from: BaseRequestTest.scala
  def connectAndReceive[T <: AbstractResponse](
                                                request: AbstractRequest,
                                                destination: SocketServer = anySocketServer,
                                                listenerName: ListenerName = listenerName
                                              )(implicit classTag: ClassTag[T], nn: NotNothing[T]): T = {
    val socket = connect(destination, listenerName)
    try sendAndReceive[T](request, socket)
    finally socket.close()
  }

  // KAFKA_TO_HSTREAM: move from: BaseRequestTest.scala
  /**
   * Serializes and sends the request to the given api.
   */
  def send(
            request: AbstractRequest,
            socket: Socket,
            clientId: String = "client-id",
            correlationId: Option[Int] = None
          ): Unit = {
    val header = nextRequestHeader(request.apiKey, request.version, clientId, correlationId)
    sendWithHeader(request, header, socket)
  }

  // KAFKA_TO_HSTREAM: move from: BaseRequestTest.scala
  def sendWithHeader(request: AbstractRequest, header: RequestHeader, socket: Socket): Unit = {
    val serializedBytes = Utils.toArray(request.serializeWithHeader(header))
    sendRequest(socket, serializedBytes)
  }

  // KAFKA_TO_HSTREAM: move from: BaseRequestTest.scala
  def nextRequestHeader[T <: AbstractResponse](
                                                apiKey: ApiKeys,
                                                apiVersion: Short,
                                                clientId: String = "client-id",
                                                correlationIdOpt: Option[Int] = None
                                              ): RequestHeader = {
    val correlationId = correlationIdOpt.getOrElse {
      this.correlationId += 1
      this.correlationId
    }
    new RequestHeader(apiKey, apiVersion, clientId, correlationId)
  }

  //   def getTopicIds(names: Seq[String]): Map[String, Uuid] = {
  //     val result = new util.HashMap[String, Uuid]()
  //     if (isKRaftTest()) {
  //       val topicIdsMap = controllerServer.controller.findTopicIds(ANONYMOUS_CONTEXT, names.asJava).get()
  //       names.foreach { name =>
  //         val response = topicIdsMap.get(name)
  //         result.put(name, response.result())
  //       }
  //     } else {
  //       val topicIdsMap = getController().kafkaController.controllerContext.topicIds.toMap
  //       names.foreach { name =>
  //         if (topicIdsMap.contains(name)) result.put(name, topicIdsMap.get(name).get)
  //       }
  //     }
  //     result.asScala.toMap
  //   }

  def getTopicIds(): Map[String, Uuid] = {
    // if (isKRaftTest()) {
    //   controllerServer.controller.findAllTopicIds(ANONYMOUS_CONTEXT).get().asScala.toMap
    // } else {
    //   getController().kafkaController.controllerContext.topicIds.toMap
    // }
    val reqVer = findProperApiVersion(ApiKeys.METADATA)
    val request = MetadataRequest.Builder.allTopics.build(reqVer)
    val response = connectAndReceive[MetadataResponse](request, anySocketServer)
    if (reqVer >= 10) { // TopicId only exists in version 10 and later
      response.data().topics().asScala.map(t => t.name() -> t.topicId()).toMap
    } else {
      response.data().topics().asScala.map(t => t.name() -> Uuid.ZERO_UUID).toMap
    }
  }

  def getTopicNames(): Map[Uuid, String] = {
    // if (isKRaftTest()) {
    //   val result = new util.HashMap[Uuid, String]()
    //   controllerServer.controller.findAllTopicIds(ANONYMOUS_CONTEXT).get().entrySet().forEach {
    //     e => result.put(e.getValue(), e.getKey())
    //   }
    //   result.asScala.toMap
    // } else {
    //   getController().kafkaController.controllerContext.topicNames.toMap
    // }
    getTopicIds().map(_.swap)
  }

  private def createBrokers(startup: Boolean): Unit = {
    // Add each broker to `brokers` buffer as soon as it is created to ensure that brokers
    // are shutdown cleanly in tearDown even if a subsequent broker fails to start
    val potentiallyRegeneratedConfigs = configs
    alive = new Array[Boolean](potentiallyRegeneratedConfigs.length)
    Arrays.fill(alive, false)
    for (config <- potentiallyRegeneratedConfigs) {
      val broker = createBrokerFromConfig(config)
      _brokers += broker
      if (startup) {
        broker.startup()
        alive(_brokers.length - 1) = true
      }
    }
  }

  private def createBrokerFromConfig(config: KafkaConfig): KafkaBroker = {
    // if (isKRaftTest()) {
    //   createBroker(config, brokerTime(config.brokerId), startup = false)
    // } else {
    //   TestUtils.createServer(
    //     config,
    //     time = brokerTime(config.brokerId),
    //     threadNamePrefix = None,
    //     startup = false,
    //     enableZkApiForwarding = isZkMigrationTest() || (config.migrationEnabled && config.interBrokerProtocolVersion.isApiForwardingEnabled)
    //   )
    // }
    createBroker(config, brokerTime(config.brokerId), startup = false)
  }

  def aliveBrokers: Seq[KafkaBroker] = {
    _brokers.filter(broker => alive(broker.config.brokerId)).toSeq
  }

//   def ensureConsistentKRaftMetadata(): Unit = {
//     if (isKRaftTest()) {
//       TestUtils.ensureConsistentKRaftMetadata(
//         aliveBrokers,
//         controllerServer
//       )
//     }
//   }
}

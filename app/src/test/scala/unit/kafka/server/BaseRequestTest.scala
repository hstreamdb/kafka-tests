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

import kafka.api.IntegrationTestHarness
import kafka.network.SocketServer
import kafka.utils.NotNothing
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.requests.{
  AbstractRequest,
  AbstractResponse,
  ApiVersionsRequest,
  ApiVersionsResponse,
  MetadataRequest,
  MetadataResponse,
  RequestHeader,
  ResponseHeader
}
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.metadata.BrokerState

import java.io.{DataInputStream, DataOutputStream}
import java.net.Socket
import java.nio.ByteBuffer
import java.util.Properties
import scala.annotation.nowarn
import scala.collection.Seq
import scala.reflect.ClassTag
import scala.jdk.CollectionConverters._

abstract class BaseRequestTest extends IntegrationTestHarness {
  private var correlationId = 0

  // If required, set number of brokers
  override def brokerCount: Int = 3

  // If required, override properties by mutating the passed Properties object
  protected def brokerPropertyOverrides(properties: Properties): Unit = {}

  override def modifyConfigs(props: Seq[Properties]): Unit = {
    props.foreach { p =>
      // p.put(KafkaConfig.ControlledShutdownEnableProp, "false")
      brokerPropertyOverrides(p)
    }
  }

  def anySocketServer: SocketServer = {
    brokers
      .find { broker =>
        val state = broker.brokerState
        state != BrokerState.NOT_RUNNING && state != BrokerState.SHUTTING_DOWN
      }
      .map(_.socketServer)
      .getOrElse(throw new IllegalStateException("No live broker is available"))
  }

//   def controllerSocketServer: SocketServer = {
//     if (isKRaftTest()) {
//      controllerServer.socketServer
//     } else {
//       servers.find { server =>
//         server.kafkaController.isActive
//       }.map(_.socketServer).getOrElse(throw new IllegalStateException("No controller broker is available"))
//     }
//   }

//   def notControllerSocketServer: SocketServer = {
//     if (isKRaftTest()) {
//       anySocketServer
//     } else {
//       servers.find { server =>
//         !server.kafkaController.isActive
//       }.map(_.socketServer).getOrElse(throw new IllegalStateException("No non-controller broker is available"))
//     }
//   }

  def brokerSocketServer(brokerId: Int): SocketServer = {
    brokers
      .find { broker =>
        broker.config.brokerId == brokerId
      }
      .map(_.socketServer)
      .getOrElse(throw new IllegalStateException(s"Could not find broker with id $brokerId"))
  }

  /**
   * Return the socket server where admin request to be sent.
   *
   * For KRaft clusters that is any broker as the broker will forward the request to the active controller. For Legacy
   * clusters that is the controller broker.
   */
  def adminSocketServer: SocketServer = {
    // if (isKRaftTest()) {
    //   anySocketServer
    // } else {
    //   controllerSocketServer
    // }
    anySocketServer
  }

  private def apiVersions: ApiVersionsResponse = {
    val request = new ApiVersionsRequest.Builder().build()
    connectAndReceive[ApiVersionsResponse](request, anySocketServer)
  }

  def findProperApiVersion(apiKey: ApiKeys): Short = {
    val version = apiVersions.apiVersion(apiKey.id)
    if (version == null) throw new IllegalArgumentException(s"API key $apiKey is not supported by the broker")
    apiKey.latestVersion().min(version.maxVersion)
  }

  def connect(socketServer: SocketServer = anySocketServer, listenerName: ListenerName = listenerName): Socket = {
    new Socket("localhost", socketServer.boundPort(listenerName))
  }

  private def sendRequest(socket: Socket, request: Array[Byte]): Unit = {
    val outgoing = new DataOutputStream(socket.getOutputStream)
    outgoing.writeInt(request.length)
    outgoing.write(request)
    outgoing.flush()
  }

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

  def sendAndReceive[T <: AbstractResponse](
      request: AbstractRequest,
      socket: Socket,
      clientId: String = "client-id",
      correlationId: Option[Int] = None
  )(implicit classTag: ClassTag[T], nn: NotNothing[T]): T = {
    send(request, socket, clientId, correlationId)
    receive[T](socket, request.apiKey, request.version)
  }

  def connectAndReceive[T <: AbstractResponse](
      request: AbstractRequest,
      destination: SocketServer = anySocketServer,
      listenerName: ListenerName = listenerName
  )(implicit classTag: ClassTag[T], nn: NotNothing[T]): T = {
    val socket = connect(destination, listenerName)
    try sendAndReceive[T](request, socket)
    finally socket.close()
  }

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

  def sendWithHeader(request: AbstractRequest, header: RequestHeader, socket: Socket): Unit = {
    val serializedBytes = Utils.toArray(request.serializeWithHeader(header))
    sendRequest(socket, serializedBytes)
  }

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

  // hstream: KafkaServerTestHarness
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

  // hstream: KafkaServerTestHarness
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

}

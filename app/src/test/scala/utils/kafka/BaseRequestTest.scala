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

}

package kafka.server

import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.metadata.BrokerState
import org.apache.kafka.common.utils.Time
import kafka.utils.Logging
import kafka.network.SocketServer

import scala.sys.process.Process

class KafkaBroker(
    val config: KafkaConfig,
    time: Time = Time.SYSTEM,
    threadNamePrefix: Option[String] = None
) extends Logging {

  def boundPort(listenerName: ListenerName): Int = socketServer.boundPort(listenerName)

  def brokerState: BrokerState = {
    // TODO
    BrokerState.RUNNING
  }

  def socketServer: SocketServer = {
    new SocketServer(config)
  }

  // TODO
  def startup() = {
    // val p = new Process("")
  }

  // TODO
  def shutdown() = {
    if (sys.env.getOrElse("CONFIGS_FILE", "").trim.nonEmpty) {
      // Delete all topics ?
    }
  }

  def awaitShutdown() = {}
}

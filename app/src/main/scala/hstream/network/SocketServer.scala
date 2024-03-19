package kafka.network

import org.apache.kafka.common.network.ListenerName
import java.net.ServerSocket
import kafka.server.KafkaConfig

class SocketServer(val config: KafkaConfig) {

  def boundPort(listenerName: ListenerName): Int = {
      val listener = config.effectiveAdvertisedListeners.find(_.listenerName == listenerName).getOrElse(
        sys.error(s"Could not find listener with name ${listenerName.value}"))
      listener.port
  }
}

object SocketServer {
  // Get an unused random tcp port
  def getRandomPort: Int = {
    val serverSocket = new ServerSocket(0)
    try {
      serverSocket.getLocalPort()
    } finally {
      serverSocket.close()
    }
  }

}

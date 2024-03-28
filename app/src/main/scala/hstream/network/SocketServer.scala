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

package kafka.server

import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.metadata.BrokerState
import org.apache.kafka.common.utils.Time
import kafka.utils.Logging
import kafka.network.SocketServer

import scala.sys.process._
import scala.util.Random

class KafkaBroker(
    val config: KafkaConfig,
    time: Time = Time.SYSTEM,
    threadNamePrefix: Option[String] = None
) extends Logging {

  def boundPort(listenerName: ListenerName): Int = socketServer.boundPort(listenerName)

  // TODO: TMP_FOR_HSTREAM
  def brokerState: BrokerState = {
    BrokerState.RUNNING
  }

  def socketServer: SocketServer = {
    new SocketServer(config)
  }

  val containerName: String = {
    val rand = Random.alphanumeric.take(10).mkString
    s"kafka-tests-${scala.util.Properties.userName}-$rand"
  }

  // TODO: TMP_FOR_HSTREAM
  def startup() = {
    if (!sys.env.getOrElse("CONFIGS_FILE", "").trim.nonEmpty) {
      // TODO
      throw new NotImplementedError("KafkaBroker.startup")
    } else {
      val spec = config.testingConfig.get("spec").asInstanceOf[Int]
      if (spec == 1) {
        val command = config.testingConfig.get("command").asInstanceOf[String]
        val image = config.testingConfig.get("image").asInstanceOf[String]
        val rmArg = if (config.testingConfig.get("container_remove").asInstanceOf[Boolean]) "--rm" else ""
        val storeDir = config.testingConfig.get("store_dir").asInstanceOf[String]
        val dockerCmd =
          s"docker run $rmArg -d --network host --name $containerName -v $storeDir:/data/store $image $command"
        dockerCmd.run()
      } else {
        throw new NotImplementedError("startup: spec is invalid!")
      }
    }
  }

  // TODO: TMP_FOR_HSTREAM
  def shutdown() = {
    if (!sys.env.getOrElse("CONFIGS_FILE", "").trim.nonEmpty) {
      // TODO
      throw new NotImplementedError("KafkaBroker.shutdown")
    } else {
      val spec = config.testingConfig.get("spec").asInstanceOf[Int]
      if (spec == 1) {
        // Remove broker container
        if (config.testingConfig.get("container_remove").asInstanceOf[Boolean]) {
          s"docker rm -f $containerName".!
        }

        // Delete all logs
        val storeAdminPort = config.testingConfig.get("store_admin_port").asInstanceOf[Int]
        val deleteLogProc =
          s"docker run --rm --network host hstreamdb/hstream bash -c 'echo y | hadmin-store --port $storeAdminPort logs remove --path /hstream -r'"
            .run()
        val code = deleteLogProc.exitValue()
        // TODO: remove a non-exist log should be OK
        // if (code != 0) {
        //  throw new RuntimeException(s"Failed to delete logs, exit code: $code")
        // }

        // Delete all metastore(zk) nodes
        val metastorePort = config.testingConfig.get("metastore_port").asInstanceOf[Int]
        s"docker run --rm --network host zookeeper:3.7 zkCli.sh -server 127.0.0.1:$metastorePort deleteall /hstream".!
      } else {
        throw new NotImplementedError("shutdown: spec is invalid!")
      }
    }
  }

  // TODO: TMP_FOR_HSTREAM
  def awaitShutdown() = {
    if (!sys.env.getOrElse("CONFIGS_FILE", "").trim.nonEmpty) {
      // TODO
      throw new NotImplementedError("KafkaBroker.awaitShutdown")
    } else {
      // TODO
    }
  }

}

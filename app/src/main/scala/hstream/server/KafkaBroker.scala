package kafka.server

import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.metadata.BrokerState
import org.apache.kafka.common.utils.Time
import java.nio.file.{Files, Path, Paths, StandardOpenOption}
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

  var logDir: Path = _

  // TODO: TMP_FOR_HSTREAM
  def startup(logPath: Path) = {
    logDir = logPath
    if (sys.env.getOrElse("CONFIG_FILE", "").trim.isEmpty) {
      // TODO
      throw new NotImplementedError("KafkaBroker.startup")
    } else {
      if (config.testingConfig.isEmpty) {
        info("No testingConfig found, skip starting broker")
      } else {
        val spec =
          config.testingConfig
            .getOrElse("spec", throw new IllegalArgumentException("spec is required"))
            .asInstanceOf[Int]
        if (spec == 1) {
          val command = config.testingConfig
            .getOrElse("command", throw new IllegalArgumentException("command is required"))
            .asInstanceOf[String]
          val image = config.testingConfig
            .getOrElse("image", throw new IllegalArgumentException("image is required"))
            .asInstanceOf[String]
          val rmArg =
            if (
              config.testingConfig
                .getOrElse("container_remove", throw new IllegalArgumentException("container_remove is required"))
                .asInstanceOf[Boolean]
            ) "--rm"
            else ""
          val storeDir = config.testingConfig
            .getOrElse("store_dir", throw new IllegalArgumentException("store_dir is required"))
            .asInstanceOf[String]
          val dockerCmd =
            s"docker run $rmArg -d --network host --name $containerName -v $storeDir:/data/store $image $command"
          info(s"=> Start hserver by: $dockerCmd")
          val code = dockerCmd.!
          if (code != 0) {
            throw new RuntimeException(s"Failed to start broker, exit code: $code")
          }
        } else {
          throw new NotImplementedError("startup: spec is invalid!")
        }
      }
    }
  }

  // TODO: TMP_FOR_HSTREAM
  def shutdown(): Unit = {
    if (sys.env.getOrElse("CONFIG_FILE", "").trim.isEmpty) {
      // TODO
      throw new NotImplementedError("KafkaBroker.shutdown")
    } else {
      if (config.testingConfig.isEmpty) {
        info("No testingConfig found, skip starting broker")
      } else {
        val spec =
          config.testingConfig
            .getOrElse("spec", throw new IllegalArgumentException("spec is required"))
            .asInstanceOf[Int]
        if (spec == 1) {
          // Dump broker container logs
          if (
            config.testingConfig
              .getOrElse("container_logs", throw new IllegalArgumentException("container_logs is required"))
              .asInstanceOf[Boolean]
          ) {
            Files.createDirectories(logDir)
            val fileName = Paths.get(s"$logDir/$containerName.log")
            if (!Files.exists(fileName)) {
              Files.createFile(fileName)
            }
//            val code = s"bash -c 'docker logs $containerName >> $fileName 2>&1'".!
            val cmd = Seq("docker", "logs", containerName)

            val processLogger = ProcessLogger(
              stdout => Files.writeString(fileName, stdout + "\n", StandardOpenOption.APPEND),
              stderr => Files.writeString(fileName, stderr + "\n", StandardOpenOption.APPEND)
            )
            info(s"get logs from $containerName in ${System.currentTimeMillis()}")
            val code = Process(cmd).!(processLogger)

            if (code != 0) {
              error(s"Failed to dump logs to $fileName, exit code: $code")
              // 执行 docker ps -a 并打印结果
                val psCmd = Seq("docker", "ps", "-a")
                val psProcessLogger = ProcessLogger(
                  stdout => info(stdout),
                  stderr => error(stderr)
                )
                Process(psCmd).!(psProcessLogger)
            } else {
              Files.writeString(fileName, "==============================================\n", StandardOpenOption.APPEND)
              info(s"Dump logs to $fileName")
            }
          }
          // Remove broker container
          if (
            config.testingConfig
              .getOrElse("container_remove", throw new IllegalArgumentException("container_remove is required"))
              .asInstanceOf[Boolean]
          ) {
            val cmd = s"docker rm -f $containerName"
            Process(cmd).!
            info(s"Remove container $containerName in ${System.currentTimeMillis()}")
            info(s"------- Current containers -------")
            val psCmd = Seq("docker", "ps", "-a")
            val psProcessLogger = ProcessLogger(
              stdout => info(stdout),
              stderr => error(stderr)
            )
            Process(psCmd).!(psProcessLogger)
            info(s"------- End of show current containers -------")
          }

//          // Delete all logs
//          val storeAdminPort = config.testingConfig
//            .getOrElse("store_admin_port", throw new IllegalArgumentException("store_admin_port is required"))
//            .asInstanceOf[Int]
//          val deleteLogProc =
//            s"docker run --rm --network host hstreamdb/hstream bash -c 'echo y | hadmin-store --port $storeAdminPort logs remove --path /hstream -r'"
//              .run()
//          val code = deleteLogProc.exitValue()
//          // TODO: remove a non-exist log should be OK
//          // if (code != 0) {
//          //  throw new RuntimeException(s"Failed to delete logs, exit code: $code")
//          // }
//
//          // Delete all metastore(zk) nodes
//          val metastorePort = config.testingConfig
//            .getOrElse("metastore_port", throw new IllegalArgumentException("metastore_port is required"))
//            .asInstanceOf[Int]
//          s"docker run --rm --network host zookeeper:3.7 zkCli.sh -server 127.0.0.1:$metastorePort deleteall /hstream".!
        } else {
          throw new NotImplementedError("shutdown: spec is invalid!")
        }
      }
    }
  }

  // TODO: TMP_FOR_HSTREAM
  def awaitShutdown() = {
    if (sys.env.getOrElse("CONFIG_FILE", "").trim.isEmpty) {
      // TODO
      throw new NotImplementedError("KafkaBroker.awaitShutdown")
    } else {
      // TODO
    }
  }

}

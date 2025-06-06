package kafka.server

import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.metadata.BrokerState
import org.apache.kafka.common.utils.Time

import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import java.io.{ByteArrayOutputStream, PrintWriter}
import kafka.utils.Logging
import kafka.network.SocketServer

import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.util.{Success, Try}
import scala.concurrent.duration._
import scala.sys.process._
import scala.util.Random

object Utils extends Logging {
  def runCommand(
      cmd: String,
      check: Boolean = true,
      captureOut: Boolean = false
  ): (Int, Option[String], Option[String]) = {
    val result =
      if (captureOut) {
        val stdoutStream = new ByteArrayOutputStream
        val stderrStream = new ByteArrayOutputStream
        val stdoutWriter = new PrintWriter(stdoutStream)
        val stderrWriter = new PrintWriter(stderrStream)
        val exitValue = cmd ! (ProcessLogger(stdoutWriter.println, stderrWriter.println))
        stdoutWriter.close()
        stderrWriter.close()
        (exitValue, Some(stdoutStream.toString), Some(stderrStream.toString))
      } else {
        val exitValue = cmd.!
        (exitValue, None, None)
      }
    if (check) {
      if (result._1 != 0) {
        throw new RuntimeException(s"Failed to run command: $cmd")
      }
    }
    result
  }

  def containerExist(name: String): Boolean = {
    val (ret, _, _) = Utils.runCommand(
      s"bash -c 'docker container inspect $name >/dev/null 2>&1'",
      captureOut = false,
      check = false
    )
    ret == 0
  }

  def waitPort(port: Int, retries: Int = 20): Boolean = {
    if (retries <= 0) {
      false
    } else {
      val f = Future {
        try {
          val socket = new java.net.Socket("127.0.0.1", port)
          socket.close()
          info("=> The port is open at " + port)
          true
        } catch {
          case e: java.net.ConnectException =>
            info("=> Retry to connect to the port " + port)
            Thread.sleep(1000)
            waitPort(port, retries - 1)
        }
      }
      Await.result(f, 60.second)
    }
  }
}

object KafkaBroker extends Logging {

  def awaitCluster(num: Int, configs: scala.collection.Seq[KafkaConfig], timeout: Int = 30): Unit = {
    if (configs.isEmpty) {
      throw new RuntimeException("No broker configs found!")
    }

    if (configs.head.testingConfig.isEmpty) {
      info("No testingConfig found, skip awaitCluster")
      return
    }
    val initPort = configs.head.port
    val image = configs.head.testingConfig
      .getOrElse("image", throw new IllegalArgumentException("image is required"))
      .asInstanceOf[String]
    val spec = configs.head.testingConfig
      .getOrElse("spec", throw new IllegalArgumentException("spec is required"))
      .asInstanceOf[Int]

    if (spec == 1) {
      initCluster(initPort)
      // TODO: Theoretically, it is adequate to ask any node to check the cluster status.
      //       However, due to the limitation of the current implementation, the cluster
      //       status may be different between different nodes'views. This can cause infinite
      //       block in some edge cases (lookup resources).
      for (config <- configs) {
        awaitNode(image, "hstream-kafka", num, config.port, timeout)
      }
    } else if (spec == 2) {
      // TODO: Theoretically, it is adequate to ask any node to check the cluster status.
      //       However, due to the limitation of the current implementation, the cluster
      //       status may be different between different nodes'views. This can cause infinite
      //       block in some edge cases (lookup resources).
      for (config <- configs) {
        awaitNode(image, "hornbill ctl", num, config.port, timeout)
      }
    } else if (spec == 3) {
      // TODO: flowmq does not support cluster yet
    } else {
      throw new NotImplementedError("awaitCluster: spec is invalid!")
    }
  }

  private def initCluster(port: Int): Unit = {
    Utils.runCommand(
      s"docker run --rm --network host hstreamdb/hstream hstream-kafka --port $port node init"
    )
  }

  private def awaitNode(cliImage: String, cliExe: String, num: Int, port: Int, timeout: Int = 30): Unit = {
    if (timeout <= 0) {
      throw new RuntimeException("Failed to start hstream cluster!")
    }
    val f = Future {
      try {
        // FIXME: better way to check cluster is ready
        val (_, nodeStatusOutOpt, _) = Utils.runCommand(
          s"docker run --rm --network host $cliImage $cliExe --port $port node status",
          captureOut = true,
          check = false
        )
        val nodeStatusOut = nodeStatusOutOpt.get.trim.split("\n")
        var numRunningNodes = 0
        for (line <- nodeStatusOut) {
          if (line.trim.startsWith("|")) {
            info(s"=> ${line}")
            if (line.contains("Running")) {
              numRunningNodes += 1
            }
          }
        }
        if (numRunningNodes < num) {
          throw new RuntimeException("Not enough running nodes")
        }
      } catch {
        case e: Exception => {
          info("=> Waiting broker ready...")
          Thread.sleep(2000)
          awaitNode(cliImage, cliExe, num, port, timeout - 2)
        }
      }
    }
    Await.result(f, 10.second)
  }
}

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
        // === common
        val command = config.testingConfig
          .getOrElse("command", throw new IllegalArgumentException("command is required"))
          .asInstanceOf[String]
        val image = config.testingConfig
          .getOrElse("image", throw new IllegalArgumentException("image is required"))
          .asInstanceOf[String]
        val extraProps = config.hstreamKafkaBrokerProperties
          .map { case (k, v) => s"--prop $k=$v" }
          .mkString(" ")
        // === spec 1: hstream
        if (spec == 1) {
          val storeDir = config.testingConfig
            .getOrElse("store_dir", throw new IllegalArgumentException("store_dir is required"))
            .asInstanceOf[String]
          val dockerCmd =
            s"docker run -d --network host --name $containerName -v $storeDir:/data/store $image $command $extraProps"
          info(s"=> Start hserver by: $dockerCmd")
          val code = dockerCmd.!
          if (code != 0) {
            throw new RuntimeException(s"Failed to start broker, exit code: $code")
          }
        }
        // === spec 2: hornbill
        else if (spec == 2) {
          val storeConfig = config.testingConfig
            .getOrElse("store_config", throw new IllegalArgumentException("store_config is required"))
            .asInstanceOf[String]
          val metaServerPort = config.testingConfig
            .getOrElse("metaserver_port", throw new IllegalArgumentException("metaserver_port is required"))
            .asInstanceOf[Int]
          val metaServerContainerName = config.testingConfig
            .getOrElse(
              "metaserver_container_name",
              throw new IllegalArgumentException("metaserver_container_name is required")
            )
            .asInstanceOf[String]
          // We only start one meta server for all brokers
          if (!Utils.containerExist(metaServerContainerName)) {
            val metaServerCmd =
              s"""docker run -d --network host --name $metaServerContainerName -v $storeConfig:$storeConfig:ro
                $image hornbill meta --host 127.0.0.1 --port $metaServerPort
                --metrics-port 0
                --backend $storeConfig
            """.stripMargin.linesIterator.mkString(" ").trim
            info(s"=> Start meta server by: $metaServerCmd")
            Utils.runCommand(metaServerCmd)
          }
          info("=> Wait for meta server ready...")
          val waitMetaRet = Utils.waitPort(metaServerPort)
          if (!waitMetaRet) {
            throw new RuntimeException("Failed to start meta server!")
          }
          val hserverCmd =
            s"docker run -d --network host --name $containerName -v $storeConfig:$storeConfig:ro $image $command $extraProps"
          info(s"=> Start hserver by: $hserverCmd")
          Utils.runCommand(hserverCmd)
        }
        // === spec 3: flowmq
        else if (spec == 3) {
          val extraFlowmqProps = config.hstreamKafkaBrokerProperties
            .map { case (k, v) => { val newk = s"$k".replace(".", "-"); s"--knob-kafka-$newk $v" } }
            .mkString(" ")
          val storeConfig = config.testingConfig
            .getOrElse("store_config", throw new IllegalArgumentException("store_config is required"))
            .asInstanceOf[String]
          val dockerCmd =
            s"docker run -d --network host --name $containerName -v $storeConfig:$storeConfig $image $command $extraFlowmqProps"
          info(s"=> Start flowmq by: $dockerCmd")
          val code = dockerCmd.!
          if (code != 0) {
            throw new RuntimeException(s"Failed to start broker, exit code: $code")
          }
        } else {
          throw new NotImplementedError("startup: spec is invalid!")
        }
      }
    }

    info("=> Wait for server ready...")
    awaitStartup()
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
        // === common
        // Dump broker container logs
        if (
          config.testingConfig
            .getOrElse("container_logs", throw new IllegalArgumentException("container_logs is required"))
            .asInstanceOf[Boolean]
        ) {
          info("=> dump container logs...")
          dumpContainerLogs()
        }
        // Remove broker container
        if (
          config.testingConfig
            .getOrElse("container_remove", throw new IllegalArgumentException("container_remove is required"))
            .asInstanceOf[Boolean]
        ) {
          info(s"=> Remove container $containerName...")
          s"docker rm -f $containerName".!
        }
        // === spec 1: hstream
        if (spec == 1) {
          // Delete all logs
          val storeAdminPort = config.testingConfig
            .getOrElse("store_admin_port", throw new IllegalArgumentException("store_admin_port is required"))
            .asInstanceOf[Int]
          val deleteLogProc =
            s"docker run --rm --network host hstreamdb/hstream bash -c 'echo y | hadmin-store --port $storeAdminPort logs remove --path /hstream -r'"
              .run()
          info("=> Delete all hstore logs...")
          val code = deleteLogProc.exitValue()
          // TODO: remove a non-exist log should be OK
          // if (code != 0) {
          //  throw new RuntimeException(s"Failed to delete logs, exit code: $code")
          // }

          // Delete all metastore(zk) nodes
          val metastorePort = config.testingConfig
            .getOrElse("metastore_port", throw new IllegalArgumentException("metastore_port is required"))
            .asInstanceOf[Int]
          info("=> Delete all zk nodes...")
          // Use the same zk version as scripe/dev-tools to avoid pulling new image
          s"docker run --rm --network host zookeeper:3.6 zkCli.sh -server 127.0.0.1:$metastorePort deleteall /hstream".!
        }
        // === spec 2: hornbill
        else if (spec == 2) {
          // Remove meta server container
          val metaServerContainerName = config.testingConfig
            .getOrElse(
              "metaserver_container_name",
              throw new IllegalArgumentException("metaserver_container_name is required")
            )
            .asInstanceOf[String]
          info("=> Remove meta server container...")
          // FIXME: check should be false, because the meta server may be removed by other brokers
          Utils.runCommand(s"docker rm -f $metaServerContainerName", check = false)
          // Remove storage
          val storeConfig = config.testingConfig
            .getOrElse("store_config", throw new IllegalArgumentException("store_config is required"))
            .asInstanceOf[String]
          val storeRmCmd = config.testingConfig
            .getOrElse("store_rm_command", throw new IllegalArgumentException("store_rm_command is required"))
            .asInstanceOf[String]
            .replace("${store_config}", storeConfig)
          info("=> Delete all data in storage...")
          Utils.runCommand(storeRmCmd)
        }
        // === spec 3: flowmq
        else if (spec == 3) {
          // Remove storage
          val storeConfig = config.testingConfig
            .getOrElse("store_config", throw new IllegalArgumentException("store_config is required"))
            .asInstanceOf[String]
          val storeRmCmd = config.testingConfig
            .getOrElse("store_rm_command", throw new IllegalArgumentException("store_rm_command is required"))
            .asInstanceOf[String]
            .replace("${store_config}", storeConfig)
          info(s"=> Delete all data in storage by command: $storeRmCmd")
          Utils.runCommand(storeRmCmd)
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

  // --------------------------------------------------------------------------
  // For hstream

  private def awaitStartup(retries: Int = 20): Unit = {
    val port = config.port
    val ret = Utils.waitPort(port, retries)
    if (!ret) {
      s"docker logs $containerName".!
      throw new RuntimeException("Failed to start the broker!")
    }
  }

  private def dumpContainerLogs() = {
    val containerLogsDir = config.testingConfig
      .getOrElse("container_logs_dir", throw new IllegalArgumentException("container.logs_dir is required"))
      .asInstanceOf[Path]
    Files.createDirectories(containerLogsDir)

    // FIXME: use "docker logs" may cause incomplete logs (? need to investigate)
    //
    // s"bash -c 'docker logs $containerName >> $containerLogsDir/$containerName.log 2>&1'".!

    val fileName = Paths.get(s"$containerLogsDir/$containerName.log")
    if (!Files.exists(fileName)) {
      Files.createFile(fileName)
    }

    val writer = Files.newBufferedWriter(fileName, StandardOpenOption.APPEND)
    val processLogger = ProcessLogger(
      stdout => writer.write(stdout + "\n"),
      stderr => writer.write(stderr + "\n")
    )

    try {
      val cmd = Seq("docker", "logs", containerName)
      val code = Process(cmd).!(processLogger)
      if (code != 0) {
        error(s"Failed to dump logs to $fileName, exit code: $code")
      } else {
        // add a separator line to separate logs from different runs
        writer.write("\n=================================================\n\n")
        info(s"Dump logs to $fileName")
      }
    } catch {
      case e: Exception =>
        error(s"Failed to dump logs to $fileName, error: $e")
    } finally {
      writer.flush()
      writer.close()
    }
  }

}

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.server

import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.metadata.BrokerState
import org.apache.kafka.common.utils.Time

import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import kafka.utils.Logging
import kafka.network.SocketServer

import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.concurrent.duration._
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
          val storeDir = config.testingConfig
            .getOrElse("store_dir", throw new IllegalArgumentException("store_dir is required"))
            .asInstanceOf[String]
          val extraProps = config.hstreamKafkaBrokerProperties
            .map { case (k, v) => s"--prop $k=$v" }
            .mkString(" ")
          val dockerCmd =
            s"docker run -d --network host --name $containerName -v $storeDir:/data/store $image $command $extraProps"
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
        if (spec == 1) {
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
          s"docker run --rm --network host zookeeper:3.7 zkCli.sh -server 127.0.0.1:$metastorePort deleteall /hstream".!
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
    if (retries <= 0) {
      s"docker logs $containerName".!
      throw new RuntimeException("Failed to start hstream!")
    }
    val port = config.port
    val f = Future {
      try {
        val socket = new java.net.Socket("127.0.0.1", port)
        socket.close()
        info("=> The server port is open at " + port)
      } catch {
        case e: java.net.ConnectException =>
          info("=> Retry to connect to the server port " + port)
          Thread.sleep(1000)
          awaitStartup(retries - 1)
      }
    }
    Await.result(f, 60.second)
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

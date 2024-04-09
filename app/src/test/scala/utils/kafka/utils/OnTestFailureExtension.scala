package utils.kafka.utils

import kafka.integration.KafkaServerTestHarness
import org.junit.jupiter.api.extension.{ExtensionContext, TestExecutionExceptionHandler}
import java.nio.file.{Files, Paths, StandardOpenOption}
import scala.sys.process._
import kafka.utils.Logging

class OnTestFailureExtension extends TestExecutionExceptionHandler with Logging {
  override def handleTestExecutionException(context: ExtensionContext, throwable: Throwable): Unit = {
    val testInstance = context.getRequiredTestInstance
    val kafkaTests = testInstance.asInstanceOf[KafkaServerTestHarness]

    val testName = context.getTestMethod.get().getName
    val logFile = s"../.log/$testName-${System.currentTimeMillis()}"
    Files.createDirectories(Paths.get(logFile))

    kafkaTests.brokers.foreach { broker =>
      val cmd = Seq("docker", "logs", broker.containerName)
      val logPath = Paths.get(logFile).resolve(s"${broker.containerName}.log")
      if(!Files.exists(logPath)) {
        Files.createFile(logPath)
      }
      val processLogger = ProcessLogger(
        stdout => Files.writeString(logPath, stdout + "\n", StandardOpenOption.APPEND),
        stderr => Files.writeString(logPath, stderr + "\n", StandardOpenOption.APPEND)
      )
      val exitCode = Process(cmd).!(processLogger)
      info(s"get log for broker ${broker.containerName}")
    }
    info(s"dump log to dir ${Paths.get(logFile).toAbsolutePath.toString}")

    throw throwable
  }
}

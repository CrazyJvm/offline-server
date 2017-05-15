package io.growing.offline.actors

import akka.actor.Actor
import io.growing.offline.actors.JobScheduler.{JobFailed, JobSucceeded, ScheduledJob}
import io.growing.offline.api.{BaseJobContext, NextJobInfo, SparkJob, SparkJobFactory}
import io.growing.offline.models.{JobGraphNode, ScheduledJobGraphNode}
import org.slf4j.LoggerFactory

import scala.util.Try

/**
  * Created by king on 12/24/16.
  */
class JobActor(jobContext: BaseJobContext) extends Actor {
  private val logger = LoggerFactory.getLogger(getClass)

  override def receive: Receive = {
    case ScheduledJob(id, scheduledJobGraphNode) =>
      logger.info(s"$self is processing job [$id]: $scheduledJobGraphNode")
      try {
        scheduledJobGraphNode.jobGraphNode.jobType match {
          case "class" =>
            val clz = reflectJob[SparkJob](scheduledJobGraphNode.jobGraphNode.job,
              scheduledJobGraphNode.jobGraphNode.properties,
              scheduledJobGraphNode.startPos,
              scheduledJobGraphNode.stopPos)
            runSparkJob(id, clz)

          // factory生成job，使用一个id管理，只有当generateJobs返回的所有job成功之后，才返回JobSucceeded(id)消息
          case "factory" =>
            val clz = reflectJob[SparkJobFactory](scheduledJobGraphNode.jobGraphNode.job,
              scheduledJobGraphNode.jobGraphNode.properties,
              scheduledJobGraphNode.startPos,
              scheduledJobGraphNode.stopPos)
            runSparkJobFactory(id, clz)

          case "debug" =>
            // 这样在测试的时候就能暂缓，便于检查currentScheduledJogGraphs等对象的状态
            val timeout = Try(scheduledJobGraphNode.jobGraphNode.properties("timeout").toInt)
              .getOrElse(1000)
            Thread.sleep(timeout)
            logger.info(s"debug: running job ${scheduledJobGraphNode.jobGraphNode.job}, " +
              s"startPos: ${scheduledJobGraphNode.startPos}, " +
              s"stopPos: ${scheduledJobGraphNode.stopPos}")
            sender() ! JobSucceeded(id)
        }
      } catch {
        case e: Exception =>
          logger.error(s"run job [$id] failed. ", e)
      }

  }

  private def reflectJob[T](job: String, properties: Map[String, String],
                            startPos: String, stopPos: String): T = {
    val conf = properties ++ Map("startPos" -> startPos, "stopPos" -> stopPos)
    Class.forName(job)
      .getConstructor(classOf[BaseJobContext], classOf[Map[String, String]])
      .newInstance(jobContext, conf)
      .asInstanceOf[T]
  }

  private def runSparkJob(id: Int, job: SparkJob): Unit ={
    val result = job.run()
    if (result.succeeded) {
      sender() ! JobSucceeded(id)
      // SparkJob运行成功之后，如果有定义对应的返回Job，则执行该部分Job，以OtherJobs的方式管理
      result.nextJobs.foreach { job =>
        sender() ! ScheduledJobGraphNode(job.startPos, job.stopPos,
          JobGraphNode(job.job, job.jobType, job.properties, job.priority))
      }
    } else {
      sender() ! JobFailed(id, result.reason.map(_.getMessage).get)
    }
  }

  private def runSparkJobFactory(id: Int, job: SparkJobFactory): Unit = {
    val nextJobs: Seq[NextJobInfo] = job.generateJobs().flatMap { job =>
      val clz = reflectJob[SparkJob](job.job, job.properties, job.startPos, job.stopPos)
      val result = clz.run()
      if (result.succeeded) {
        result.nextJobs
      } else {
        sender() ! JobFailed(id, result.reason.map(_.getMessage).get)
        // 跳转到catch，避免提交后续的nextJobs
        throw new Exception(s"job[$id] factory failed, when running " +
          s"${job.job}, ${job.startPos}, ${job.stopPos}, ${job.properties}", result.reason.get)
      }
    }
    // 顺利运行到此处
    sender() ! JobSucceeded(id)
    nextJobs.foreach { job =>
      sender() ! ScheduledJobGraphNode(job.startPos, job.stopPos,
        JobGraphNode(job.job, job.jobType, job.properties, job.priority))
    }
  }
}

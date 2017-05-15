package io.growing.offline.actors

import java.time.ZoneId
import java.util.concurrent.ConcurrentHashMap

import akka.actor.{Actor, ActorRef, Props, Terminated}
import com.fasterxml.jackson.databind.ObjectMapper
import com.typesafe.config.Config
import io.growing.offline.actors.JobScheduler._
import io.growing.offline.actors.Submitter.SubmitJobFinished
import io.growing.offline.models._
import io.growing.offline.services.SqlService
import io.growing.offline.utils.ConfigUtils
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try

/**
  * Created by king on 12/21/16.
  * scheduler主要用于任务的持久化以及恢复
  */
class JobScheduler(config: Config, jobConfigs: Set[JobConfig],
                   sqlService: SqlService)
                  (implicit timeZone: ZoneId) extends Actor {
  private val logger = LoggerFactory.getLogger(getClass)
  implicit val ec = context.dispatcher

  private var submitter: ActorRef = null
  private var jobServer: ActorRef = null

  private val mapper: ObjectMapper = ConfigUtils.mapper

  private val jobInfo: Map[String, JobInfo] = jobConfigs.map(j => j.name -> j.jobInfo).toMap

  private val currentOtherJobs: ConcurrentHashMap[Int, ScheduledJobGraphNode] =
    new ConcurrentHashMap[Int, ScheduledJobGraphNode]()

  private val currentScheduledJobGraphs: ConcurrentHashMap[Int, (String, ScheduledJobGraph)] =
    new ConcurrentHashMap[Int, (String, ScheduledJobGraph)]()

  private val maxRetries: Int = Try(config.getInt("maxRetries")).getOrElse(1)
  // id -> retried
  private val retriedJobs: mutable.HashMap[Int, Int] = mutable.HashMap.empty

  override def receive: Receive = {
    // JobServer的启动与停止
    case StartScheduler(_submitter) =>
      submitter = _submitter
      self ! StartJobServer
      self ! RecoveryScheduler

    case RecoveryScheduler =>
      // 进行初始化,恢复上次暂停的任务，需要从offline_jobs恢复出ScheduledJobGraph
      val recoveryFuture = sqlService.queryAllJobs().map(recovery)
      Await.result(recoveryFuture, 20.seconds)
      // 将currentOtherJobs与currentScheduledJobGraphs内的任务发送到jobServer
      currentOtherJobs.asScala.foreach(j => jobServer ! ScheduledJob(j._1, j._2))
      currentScheduledJobGraphs.asScala.map(_._2._2).toSet.foreach { graph: ScheduledJobGraph =>
        graph.scheduleJobs().foreach(j => jobServer ! j)
      }

    // 这样就可以重启SparkContext,替换Jobs jar包???。。。
    case StopJobServer =>
      context.stop(jobServer)

    case StartJobServer =>
      startJobServer()

    // 普通的任务，纪录在currentOtherJobs中
    case node: ScheduledJobGraphNode =>
      sqlService.createJob(
        node.startPos,
        node.stopPos,
        node.jobGraphNode).map { id =>
        currentOtherJobs.put(id, node)
        jobServer ! ScheduledJob(id, node)
      }

    // scheduledJobGraph类型的任务
    case scheduledJobGraph: ScheduledJobGraph =>    // 纯异步操作的写法...
      // 将ScheduledJobGraph的任务写入到offline_jobs表
      sqlService.createJobs(scheduledJobGraph.name,
        scheduledJobGraph.startPos,
        scheduledJobGraph.stopPos,
        scheduledJobGraph.jobGraph.jobNodes.values.toSeq)
        .map { jobIds =>
          // 持久化到数据库之后，初始化ScheduledJobGraph，使其带上id信息
          scheduledJobGraph.initial(jobIds.toMap)

          // 注册job对应ScheduledJobGraph，在JobScheduler中，用数据库内的id唯一标识job
          jobIds.foreach { j =>
            currentScheduledJobGraphs.put(j._2, (j._1, scheduledJobGraph))
          }

          // 将ScheduledJob发送给JobServer，必须在initial之后
          scheduledJobGraph.scheduleJobs().foreach(jobServer ! _)
      }

    case JobSucceeded(id) =>
      sqlService.updateJobFinished(id).map { _ =>
        logger.info(s"job [$id] completed successfully.")
        // 如果是terminalJob，需要往对应的submit发送SubmitJobFinished消息
        if (currentScheduledJobGraphs.containsKey(id)) {
          val (jobNodeId, scheduledJobGraph) = currentScheduledJobGraphs.get(id)
          if (scheduledJobGraph.terminalJob == null && scheduledJobGraph.jobGraphFinished() ||
            jobNodeId == scheduledJobGraph.terminalJob) {
            submitter ! SubmitJobFinished(scheduledJobGraph.name)
          }

          // 更新对应的ScheduledJobGraph内的marked
          scheduledJobGraph.updateMark(jobNodeId)

          // 检查jobGraph是否还有待提交的任务，若有则提交可以提交的任务
          if (!scheduledJobGraph.jobGraphFinished()) {
            scheduledJobGraph.scheduleJobs().foreach(jobServer ! _)
          }

          // 清除该id
          currentScheduledJobGraphs.remove(id)
        } else if (currentOtherJobs.containsKey(id)) {   // 属于otherJobs，不走Graph的情况
          // 清除该id
          currentOtherJobs.remove(id)
        } else {
          logger.warn(s"unknown job id[$id], not in currentScheduledJobGraphs or currentOtherJobs.")
        }

        // 如果该任务是失败过重新运行，则需要清理retriedJobs
        if (retriedJobs.contains(id)) {
          logger.info(s"job [$id] succeeded after retried ${retriedJobs(id)} times.")
          retriedJobs.remove(id)
        }

        // 如果是失败重跑，id依然在currentScheduledJobGraphs内
        // 如果是成功的任务重跑，id已经不在currentScheduledJobGraphs内，仅会算是单独运行该任务
      }


    case JobFailed(id, reason) =>
      logger.warn(s"job [$id] failed. reason: $reason.")
      sqlService.updateJobFailed(id, reason).map { _ =>
        val retried = retriedJobs.getOrElse(id, 0)
        if (retried < maxRetries) {
          retriedJobs += id -> (retried + 1)
          if (currentScheduledJobGraphs.contains(id)) {
            val (jobNodeId, scheduledJobGraph) = currentScheduledJobGraphs.get(id)
            val newScheduledJob = ScheduledJob(id, scheduledJobGraph.getScheduledJobGraphNode(jobNodeId))
            jobServer ! newScheduledJob
            logger.warn(s"graph job $jobNodeId[$id] restarted.")
          } else if (currentOtherJobs.contains(id)) {
            jobServer ! ScheduledJob(id, currentOtherJobs.get(id))
            logger.warn(s"other job ${currentOtherJobs.get(id).jobGraphNode.job}[$id] restarted.")
          }
        } else {
          retriedJobs.remove(id)
          logger.error(s"job [$id] failed after retried $maxRetries times. reason: $reason")
        }

      }

    // 重启某个失败的job，如果job成功就会移到offline_jobs_done表内，重启失败
    case RestartJob(id) =>
      val _sender = sender()
      // 从数据库获取job信息
      sqlService.queryJobById(id).map {
        case Some(j) =>
          val properties = mapper.readValue(j.properties, classOf[Map[String, String]])
          val scheduledJobGraphNode = ScheduledJobGraphNode(
            j.startPos, j.stopPos,
            JobGraphNode(j.job, j.jobType, properties, j.priority))
          currentOtherJobs.put(id, scheduledJobGraphNode)
          jobServer ! ScheduledJob(id, scheduledJobGraphNode)

        case None =>
          _sender ! RestartJobFailed(id, s"could not find job [$id] in database.")
          logger.warn(s"restart failed. Could not found job [$id] in database.")
      }

    case Terminated(actor) =>
      logger.info(s"actor $actor is terminated.")


    // 查询scheduler状态的消息处理，便于测试监控
    case ScheduledJobGraphsList =>
      sender() ! currentScheduledJobGraphs.asScala.values.map(j =>
        (j._2.name, j._2.startPos, j._2.stopPos, j._1))
        .groupBy(j => (j._1, j._2, j._3))
        .map(k => CurrentScheduledJobGraphInfo(k._1._1, k._1._2, k._1._3, k._2.map(_._4).toSeq))

    case OtherJobsList =>
      sender() ! currentOtherJobs.asScala.values.map(j =>
        CurrentOtherJobInfo(j.jobGraphNode.job,
          j.startPos, j.stopPos, j.jobGraphNode.properties)).toSeq

  }

  private def startJobServer(): Unit = {
    jobServer = context.actorOf(Props(classOf[JobServer], config.getConfig("jobServer"),
      sqlService, timeZone), "jobServer")
    context.watch(jobServer)
  }

  private def report(message: String): Unit = {
//    if (wechatClientService.isDefined) {
//      wechatClientService.get.send(message)
//    }
  }

  // 从数据库中恢复graphs
  private def recovery(records: Seq[JobTableRecord]): Unit = {
    records.filter(_.submit_name.isDefined)
      .groupBy(r => (r.submit_name, r.startPos, r.stopPos)).foreach { j =>
      val originalJobInfo = jobInfo(j._1._1.get)
      val originalGraph = ScheduledJobGraph(j._1._1.get, j._1._2, j._1._3,
        originalJobInfo.jobGraph, originalJobInfo.terminalJob)
      val jobIds = j._2.map(k => k.job -> k.id)
      originalGraph.initial(jobIds.toMap)

      jobIds.foreach(k => currentScheduledJobGraphs.put(k._2, (k._1, originalGraph)))
    }

    records.filter(_.submit_name.isEmpty).foreach { j =>
      val properties = mapper.readValue(j.properties, classOf[Map[String, String]])
      val scheduledJobGraphNode = ScheduledJobGraphNode(
        j.startPos, j.stopPos,
        JobGraphNode(j.job, j.jobType, properties, j.priority))
      currentOtherJobs.put(j.id, scheduledJobGraphNode)
    }
  }

}

object JobScheduler {
  case class StartScheduler(submitter: ActorRef)

  case object StopJobServer
  case object StartJobServer
  case object RecoveryScheduler

  case class ScheduledJob(id: Int, jobNode: ScheduledJobGraphNode)

  // 重启任务
  case class RestartJob(id: Int)
  case object RestartJobSucceeded
  case class RestartJobFailed(id: Int, reason: String)

  sealed trait JobStatus
  // 任务成功,更新数据库状态
  case class JobSucceeded(id: Int) extends JobStatus
  // 失败了rerun???
  case class JobFailed(id: Int, reason: String) extends JobStatus

  // 查询scheduledJobGraph
  case object ScheduledJobGraphsList
  case class CurrentScheduledJobGraphInfo(name: String, startPos: String, stopPos: String, jobNodeIds: Seq[String])

  // 查询otherJobs
  case object OtherJobsList
  case class CurrentOtherJobInfo(jobName: String, startPos: String, stopPos: String, properties: Map[String, String])

  // 用于测试recovery
  case object CleanAllJobs
}

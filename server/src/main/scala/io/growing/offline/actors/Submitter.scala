package io.growing.offline.actors

import java.time.temporal.ChronoUnit
import java.time.{Instant, ZoneId, ZonedDateTime}
import java.util.concurrent.ConcurrentHashMap

import akka.actor.{Actor, FSM, _}
import com.typesafe.config.Config
import io.growing.offline.actors.Submitter._
import io.growing.offline.models._
import io.growing.offline.services.{KafkaService, SqlService}
import io.growing.offline.utils.ConfigUtils
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

/**
  * Created by king on 12/21/16.
  * 检查所有的topic&group的offsets(从kafka_offsets表获取最近的一条)
  * 若有符合条件的任务,则提交任务给JobScheduler
  *
  */
class Submitter(config: Config, jobConfigs: Set[JobConfig],
                sqlService: SqlService, kafkaService: KafkaService)
               (implicit timeZone: ZoneId) extends Actor with FSM[State, Data]{
  private val logger = LoggerFactory.getLogger(getClass)

  private val submitterInterval: Int = config.getInt("interval")
  private val timeout = 10.seconds

  private var scheduler: ActorRef = null

  implicit val executor = context.dispatcher

  // 初始化从jobConfigs读取需要提交的任务
  private val jobSubmitInfo: ConcurrentHashMap[String, SubmitInfo] = {
    val _jobSubmitInfo = new ConcurrentHashMap[String, SubmitInfo](16)
    jobConfigs.foreach(f => _jobSubmitInfo.put(f.name, f.submitInfo))
    _jobSubmitInfo
  }

  private val jobInfo: ConcurrentHashMap[String, JobInfo] = {
    val _jobInfo = new ConcurrentHashMap[String, JobInfo](16)
    jobConfigs.foreach(f => _jobInfo.put(f.name, f.jobInfo))
    _jobInfo
  }

  private val jobSubmitIds: ConcurrentHashMap[String, Int] =
    new ConcurrentHashMap[String, Int](16)

  private val blockedJobs: mutable.Set[String] = mutable.Set.empty

  // 从数据库恢复上一次的运行状态,读取上一次成功的任务与时间点
  // Long代表下一个任务提交需要满足的时间点, 单位为秒，即stopPos所在的时间点
  // Boolean代表上一个任务是否已经完成,由Scheduler传回
  // 两个条件都满足时才执行下一个任务
  private val jobSubmitters: mutable.HashMap[String, (Long, Boolean)] = mutable.HashMap.empty
  // 从数据库恢复or初始化
  jobSubmitInfo.keySet().asScala.foreach(initialJobSubmitters)

  startWith(Idle, Uninitialized)

  when(Idle) {
    // 通过Start启动Submitter
    case Event(StartSubmitter(_scheduler), Uninitialized) =>
      logger.debug(s"received event StartSubmitter in Uninitialized state.")
      // ping-pong的方式传入sibling actor
      scheduler = _scheduler
      // 初始化时,检查是否可以提交任务
      goto(Active) using CurrentTime()
    case Event(Flush, Initialized) =>
      // 切入到该状态可以暂停所有的任务，此处还没想好怎么用
      goto(Active) using CurrentTime()
  }

  // 在Active状态,检查所有的topicAndGroups
  // 若有需要提交的任务,则提交至scheduler
  // 操作完成后一直停留在Active
  // 设置一个2 minute的timer
  when(Active, stateTimeout = submitterInterval.seconds) {
    case Event(Flush | StateTimeout, t: CurrentTime) =>
      logger.debug(s"now in Active state with current time: ${ConfigUtils.parseTime2Pos(t.currentTime)}.")
      val newCurrentTime = CurrentTime()
      scheduleJob(newCurrentTime.currentTime)
      stay() using newCurrentTime
  }

  // 写着测试FSM变化
  onTransition {
    case Idle -> Active => nextStateData match {
      case c: CurrentTime =>
        logger.debug(s"Idle -> Active, now on transition with current time ${c.currentTime}.")
      case _ =>
        logger.debug(s"Idle -> Active, unknown state data $stateData.")
    }
    case Active -> Idle => stateData match {
      case c: CurrentTime =>
        logger.debug(s"Active -> Idle, now on transition with current time ${c.currentTime}.")
      case _ =>
        logger.debug(s"Active -> Idle, unknown state data $stateData.")
    }
  }

  // 管理当前submitter内所有的jobConfigs
  whenUnhandled {
    // 列出当前所有的JobConfig
    case Event(SubmitJobConfigList, _) =>
      val currentJobConfigs = jobSubmitInfo.asScala
        .map(j => JobConfig(j._1, jobInfo.get(j._1), j._2)).toSet
      stay() replying currentJobConfigs
    case Event(SubmitJobList, _) =>
      // 返回左闭右开的区间
      val currentJobSubmitters = jobSubmitters.map { j =>
        j._1 -> (ConfigUtils.parseTime2Pos(j._2._1), j._2._2)
      }.toMap
      stay() replying currentJobSubmitters
    case Event(SubmitJobRemove(name), _) =>
      jobSubmitInfo.remove(name)
      jobSubmitters.remove(name)
      logger.info(s"current submit jobs left: ${jobSubmitInfo.asScala}")
      stay()
    case Event(SubmitJobAdd(jobConfig), _) =>
      jobSubmitInfo.put(jobConfig.name, jobConfig.submitInfo)
      jobInfo.put(jobConfig.name, jobConfig.jobInfo)
      initialJobSubmitters(jobConfig.name)
      logger.info(s"add new jobConfig with name: ${jobConfig.name}, " +
        s"first stopPos = ${ConfigUtils.parseTime2Pos(jobSubmitters(jobConfig.name)._1)}")
      stay()
    case Event(SubmitJobFinished(name), _) =>
      jobSubmitters.synchronized {
        // 有可能此任务已经被删除,不需要再运行后续的程序
        if (jobSubmitters.contains(name)) {
          jobSubmitters.put(name, (jobSubmitters(name)._1, true))
        }
      }
      stay()
    case Event(SubmitJobBlock(name), _) =>
      blockedJobs.synchronized(blockedJobs.add(name))
      logger.info(s"$name is blocked.")
      stay()

    case Event(SubmitJobResume(name), _) =>
      blockedJobs.synchronized(blockedJobs.remove(name))
      logger.info(s"$name is resumed.")
      stay()

    case Event(e, s) =>
      logger.warn(s"received unhandled request $e in state $stateName/$s")
      stay()
  }

  initialize()

  // 如果有未完成的submit job，则代表该job graph的任务已经添加到offline_jobs表内，从该时间点恢复
  // 如果上一个submit job已经完成，当前的还未创建（处于Idle状态，还未检查下一个状态）
  // 如果没有上一个submit job，新建的submit job，则提交上一个interval的任务，这样便于debug
  private def initialJobSubmitters(submitName: String): Unit = {
    val interval = jobSubmitInfo.get(submitName).interval
    val future: Future[Unit] = sqlService.queryLastSubmitJobTimePos(submitName).flatMap {
      case Some((pos, id)) =>
        logger.info(s"find last unfinished submit job[$id]: $submitName")
        jobSubmitIds.put(submitName, id)
        val unfinishedTime = ConfigUtils.parsePos2Time(pos)
        // 此时上一个任务还未结束，所以false
        jobSubmitters += submitName -> (nextTimestamp(interval, unfinishedTime), false)
        Future.successful()
      case None =>
        // 之前的任务已经完成，找上一个最近完成的time pos
        sqlService.queryLastSubmitJobTimePos(submitName, finished = true).flatMap {
          case Some((pos, id)) =>
            logger.info(s"find last finished submit job[$id]: $submitName")
            val lastsubmitTime = ConfigUtils.parsePos2Time(pos)
            val finishedTime = nextTimestamp(interval, lastsubmitTime, offset = 2)
            val nextSubmitTimePos = ConfigUtils.parseTime2Pos(nextTimestamp(interval, lastsubmitTime))

            sqlService.createSubmitJob(submitName, nextSubmitTimePos).map { newId =>
              logger.info(s"created next submit job: $submitName at $nextSubmitTimePos with id = $newId.")
              jobSubmitIds.put(submitName, newId)
              // 此时上一个任务已经结束，所以为true
              jobSubmitters += submitName -> (finishedTime, true)
            }
          case None =>
            val finishedTime = nextTimestamp(interval, System.currentTimeMillis() / 1000, offset = 0)
            val nextSubmitTimePos = ConfigUtils.parseTime2Pos(nextTimestamp(interval, finishedTime, offset = -1))
            // 更新数据库
            sqlService.createSubmitJob(submitName, nextSubmitTimePos).map { id =>
              logger.info(s"created new submit job: $submitName at $nextSubmitTimePos with id = $id.")
              jobSubmitIds.put(submitName, id)
              jobSubmitters += submitName -> (finishedTime, true)
            }
        }
    }

    Await.result(future, timeout)

    logger.info(s"added new submit job: $submitName, first stopPos " +
      s"${ConfigUtils.parseTime2Pos(jobSubmitters(submitName)._1)}")
  }

  private def scheduleJob(currentTime: Long): Unit = {
    // 1. 过滤出所有上一个任务已经结束，并且不在blockedJobs内的submit任务
    // blockedJobs 不再提交
    val finishedJobSubmitters = jobSubmitters
      .filterKeys(!blockedJobs.contains(_))
      .filter(_._2._2)
      .filter(currentTime >= _._2._1)   // 时间点条件也已经满足
      .keySet
    if (finishedJobSubmitters.nonEmpty) {
      logger.debug(s"$finishedJobSubmitters last job has finished.")
    }

    // 2. 检查kafka部分的判断
    // 找出所有需要寻找的topic And groups
    val allTopicAndGroups = jobSubmitInfo.asScala
      .filter(j => finishedJobSubmitters.contains(j._1))
      .filter(_._2.topicAndGroups != null)
      .flatMap(_._2.topicAndGroups).toSet
    // 获取该TopicAndGroup对应数据消费的时间戳
    val allTimestamps = allTopicAndGroups.map { tg =>
      val future = sqlService.fetchOffsets(tg.topic, tg.group)
      val offsets = Await.result(future, 60.seconds)
      // 获取对应每个partition的offset上消息的stm
      tg -> kafkaService.fetchMessages(offsets).values.toList
    }.toMap

    finishedJobSubmitters.foreach { name =>
      // 判断kafka内消费情况
      val kafkaCondition: Boolean =
        if (jobSubmitInfo.get(name).topicAndGroups == null) {
          true  // 若为null，该条件直接为true
        } else {
          checkKafka(allTimestamps, jobSubmitters(name)._1 * 1000, name)
        }

      val parentJobCondition: Boolean =
        if (jobSubmitInfo.get(name).parent == null) {
          true
        } else {
          checkParentJob(name)
        }

      if (kafkaCondition && parentJobCondition) {
        val next = nextJob(name)
        scheduler ! next
        logger.info(s"submit $name's next job, startPos: ${next.startPos}, stopPos: ${next.stopPos}.")
      }
    }
  }

  /**
    * 判断该submit job是否所有的TopicAndGroup都已经 > currentTime
    * @param allTimestamps 过滤出来的对应submit job的所有timestamp
    * @param time 当前需要比对的时间点
    * @return
    */
  private def checkKafka(allTimestamps: Map[TopicAndGroup, List[Long]], time: Long, submitName: String): Boolean = {
    val myTimestamps = allTimestamps.filterKeys(jobSubmitInfo.get(submitName).topicAndGroups.contains(_))

    myTimestamps.count { t =>
      t._2.nonEmpty && {
        val passedTime = t._2.filter(_ > time)
        logger.info("topic {} and group {} not passed for submit name {}, passed ratio: {}.",
          t._1.topic, t._1.group, submitName, (passedTime.size * 1.0 / t._2.size).toString)
        passedTime.size == t._2.size
      }
    } == myTimestamps.size
  }

  /**
    * 判断当前任务的父任务是否已经完成，判断条件为当前任务的stopPos < parent job's startPos
    * @param name 当前任务名称
    * @return
    */
  private def checkParentJob(name: String): Boolean = {
    val parent = jobSubmitInfo.get(name).parent

    val myTimestamp: Long = jobSubmitters(name)._1
    val parentStartTimestamp: Long = nextTimestamp(
      jobSubmitInfo.get(parent).interval, jobSubmitters(parent)._1, -1)

    val passed = myTimestamp <= parentStartTimestamp && jobSubmitters(parent)._2

    if (passed) {
      logger.info(s"$name's parent job $parent finished at ${ConfigUtils.parseTime2Pos(parentStartTimestamp)}.")
    }
    passed
  }


  private def nextJob(submitName: String): ScheduledJobGraph = {
    val stopTime = jobSubmitters(submitName)._1
    val startTime = nextTimestamp(jobSubmitInfo.get(submitName).interval,
      jobSubmitters(submitName)._1, offset = -1)
    val stopTimePos = ConfigUtils.parseTime2Pos(stopTime)
    val startTimePos = ConfigUtils.parseTime2Pos(startTime)

    // 更新jobSubmitters到下一个时间点
    val nextStopTime = nextTimestamp(jobSubmitInfo.get(submitName).interval,
      jobSubmitters(submitName)._1)
    jobSubmitters += submitName -> (nextStopTime, false)

    // 更新原来的finished_at字段，并且在数据库中创建下一个submit job
    val createSubmitJobFuture = sqlService
      .updateSubmitJobFinished(jobSubmitIds.get(submitName))
      .flatMap { _ =>
        sqlService.createSubmitJob(submitName, stopTimePos)
          .map(id => jobSubmitIds.put(submitName, id))
    }
    Await.result(createSubmitJobFuture, timeout)

    ScheduledJobGraph(submitName,
      startTimePos,
      stopTimePos,
      jobInfo.get(submitName).jobGraph,
      jobInfo.get(submitName).terminalJob
    )
  }

  private def nextTimestamp(interval: String, ct: Long, offset: Int = 1): Long = {
    interval match {
      case "daily" =>
        toSeconds(ct, ChronoUnit.DAYS, offset)
      case "half-hourly" =>
        val res = toSeconds(ct, ChronoUnit.HOURS, offset)
        if (ct % 3600 >= 1800) {
          res + 1800
        } else {
          res
        }
      case "hourly" =>
        toSeconds(ct, ChronoUnit.HOURS, offset)
      case "monthly" =>
        ZonedDateTime.ofInstant(Instant.ofEpochSecond(ct), timeZone)
          .withDayOfMonth(1)
          .plus(offset, ChronoUnit.MONTHS).toEpochSecond
    }
  }

  private def toSeconds(ct: Long, truncateUnit: ChronoUnit, offset: Int = 1): Long = {
    ZonedDateTime.ofInstant(Instant.ofEpochSecond(ct), timeZone)
      .truncatedTo(truncateUnit)
      .plus(offset, truncateUnit).toEpochSecond
  }
}

object Submitter {
  // 运行状态
  sealed trait State
  case object Idle extends State
  case object Active extends State

  // 数据状态
  sealed trait Data
  case object Uninitialized extends Data
  case object Initialized extends Data
  case class CurrentTime(currentTime: Long = System.currentTimeMillis() / 1000) extends Data

  // 接收的消息
  case class StartSubmitter(target: ActorRef)   // 加载上次运行的任务信息
  case object SubmitJobConfigList
  case object SubmitJobList
  case class SubmitJobRemove(submitName: String)
  case class SubmitJobAdd(jobConfig: JobConfig)
  case class SubmitJobBlock(submitName: String)
  case class SubmitJobResume(submitName: String)
  // 当jobGraph的terminalJob结束时，通知submitter
  case class SubmitJobFinished(submitName: String)

  case object Flush
}
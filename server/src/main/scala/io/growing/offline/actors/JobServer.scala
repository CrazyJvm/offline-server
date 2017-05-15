package io.growing.offline.actors

import java.time.ZoneId

import akka.actor.{Actor, ActorRef, Props}
import akka.routing.FromConfig
import com.typesafe.config.Config
import io.growing.offline.actors.JobScheduler.ScheduledJob
import io.growing.offline.api.{BaseJobContext, JobContext, TestJobContext}
import io.growing.offline.services.SqlService
import io.growing.offline.utils.ConfigUtils
import org.slf4j.LoggerFactory

import scala.util.Try

/**
  * Created by king on 12/21/16.
  * 初始化 jobContext, use yarn
  * 通过配置dispatcher控制并发数
  */
class JobServer(config: Config, sqlService: SqlService,
                timeZone: ZoneId) extends Actor {
  private val logger = LoggerFactory.getLogger(getClass)

  // 初始化JobContext，此处根据配置文件创建BaseJobContext，便于测试
  var jobContext: BaseJobContext = null

  // 使用router actor,相当于创建一个actor来代替线程池
  // 没有写单独的router,主要是懒得配置
  var jobRouter: ActorRef = null

  implicit val executor = context.dispatcher

  override def preStart(): Unit = {
    val contextConfig = ConfigUtils.parseContextConfigs(config) ++
      Map("timeZone" -> timeZone.getId)
    logger.debug(s"jobContext configs: $contextConfig")
    // 创建jobContext
    val debug: Boolean = Try(config.getBoolean("debug")).getOrElse(false)
    if (debug) {
      jobContext = new TestJobContext(null, contextConfig)
    } else {
      jobContext = new JobContext(contextConfig)
    }

    jobRouter = context.actorOf(FromConfig.props(Props(classOf[JobActor], jobContext)), "jobRouter")
  }

  override def receive: Receive = {
    case job: ScheduledJob =>
      val _sender = sender()
      sqlService.updateJobStarted(job.id).map { _ =>
        logger.info(s"started job [${job.id}], info: ${job.jobNode}")
        jobRouter.tell(job, _sender)
      }

    case _ =>
      logger.warn("do not send me any unknown messages.")

  }

  override def postStop(): Unit = {
    jobContext.stop()
  }
}

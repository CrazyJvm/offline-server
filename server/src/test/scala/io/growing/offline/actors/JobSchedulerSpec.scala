package io.growing.offline.actors

import java.time.ZoneId

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.{TestActorRef, TestKit}
import com.typesafe.config.{Config, ConfigFactory}
import io.growing.offline.actors.JobScheduler._
import io.growing.offline.models.{JobGraphNode, ScheduledJobGraph}
import io.growing.offline.services.SqlService
import io.growing.offline.utils.ConfigUtils

import scala.concurrent.Await
import scala.concurrent.duration._


/**
  * Created by king on 12/26/16.
  */
class JobSchedulerSpec(_system: ActorSystem) extends BaseActorSuite(_system) {
  private var scheduler: ActorRef = null
  private var sqlService: SqlService = null
  private implicit val ec = _system.dispatcher
  private var id: Int = 0
  private val config: Config = ConfigFactory.load("application-test")
  private val jobConfigs = ConfigUtils.parseJobConfigs(config)
  private val jobInfo1 = jobConfigs.filter(_.name == "test-hourly-jobs1").head
  private val jobInfo2 = jobConfigs.filter(_.name == "test-hourly-jobs2").head
  implicit val timeZone: ZoneId = ConfigUtils.timeZone(config)

  def this() = this(ActorSystem("JobSchedulerSpec", ConfigFactory.load("application-test")))

  override def beforeAll(): Unit = {
    sqlService = new SqlService(config)
    scheduler = TestActorRef(new JobScheduler(
      config.getConfig("app.scheduler"), jobConfigs, sqlService), "scheduler")

    scheduler ! StartScheduler(testActor)
  }

  override def afterAll(): Unit = {
    sqlService.close()
    TestKit.shutdownActorSystem(system)
  }

  test("job scheduler recover successfully") {
    // 在数据库中创建对应的任务，适应jobConfigs
    val jobs1 = jobInfo1.jobInfo.jobGraph.jobNodes.values
      .filter(!_.job.startsWith("io.growing.test.TestJob0")).toSeq
    Await.result(sqlService.createJobs("test-hourly-jobs1", "2017100800", "2017100900", jobs1), 10.seconds)

    val jobs2 = jobInfo2.jobInfo.jobGraph.jobNodes.values.toSeq
    Await.result(sqlService.createJobs("test-hourly-jobs2", "2017100700", "2017100800", jobs2), 10.seconds)

    // 创建一个otherJob
    val node = JobGraphNode("other.Job", "debug", Map(), 1)
    sqlService.createJob("2017100800", "2017100900", node)

    // 通知scheduler恢复数据
    scheduler ! RecoveryScheduler

    // 检查currentScheduledJobGraph与currentOtherJobs
    scheduler ! ScheduledJobGraphsList
    val expectedResult1 = Seq(
      CurrentScheduledJobGraphInfo("test-hourly-jobs1", "2017100800", "2017100900", jobs1.map(_.job)),
      CurrentScheduledJobGraphInfo("test-hourly-jobs2", "2017100700", "2017100800", jobs2.map(_.job)))

    expectMsg(expectedResult1)

    Thread.sleep(10000)
  }

  test("job scheduler schedule graph") {
    scheduler ! ScheduledJobGraph("schedule-test", "201705150800", "201705150800",
      jobInfo1.jobInfo.jobGraph, jobInfo1.jobInfo.terminalJob)

    // 异步处理，所以得等actor执行完成
    Thread.sleep(1000)

    scheduler ! ScheduledJobGraphsList
    val expectedResult1 = Seq(
      CurrentScheduledJobGraphInfo("schedule-test", "201705150800", "201705150800",
        jobInfo1.jobInfo.jobGraph.jobNodes.values.map(_.job).toSeq)
    )
    expectMsg(expectedResult1)

    Thread.sleep(5000)
  }



  test("start job server") {
//    scheduler ! StartScheduler(testActor)
//    Thread.sleep(3000)
    println("pass")
  }

  test("restart job") {
    // 1. 先存储一条记录
//    id = Await.result(sqlService.createJob(
//      testJobInfoV1.job, testJobInfoV1.jobType, testJobInfoV1.properties),
//      10.seconds)
    scheduler ! RestartJob(id)
    expectMsg(30.seconds, RestartJobSucceeded)
  }

  test("restart job failed") {
    scheduler ! RestartJob(1000)
    expectMsg(30.seconds, RestartJobFailed(1000, s"could not find job [1000] in database."))
  }

}

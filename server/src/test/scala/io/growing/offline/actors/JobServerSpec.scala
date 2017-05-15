package io.growing.offline.actors

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.{TestActorRef, TestKit}
import com.typesafe.config.ConfigFactory
import io.growing.offline.actors.JobScheduler.JobSucceeded
import io.growing.offline.services.SqlService

/**
  * Created by king on 12/26/16.
  * 需要测试JobServer的优先级队列,线程池功能
  */
class JobServerSpec(_system: ActorSystem) extends BaseActorSuite(_system) {
  var sqlService: SqlService = null
  var jobServer: ActorRef = null
  implicit val ec = _system.dispatcher

  private val config = ConfigFactory.load("application-test")

  def this() = this(ActorSystem("JobServerSpec", ConfigFactory.load("jobServer-test")))

  override def beforeAll(): Unit = {
    sqlService = new SqlService(config)
    jobServer = TestActorRef(Props(classOf[JobServer], config, sqlService), "jobServer")
  }

  test("jobServer could process scheduled jobs") {
//    val id = Await.result(
//      sqlService.createJob(testJobInfoV1.job, testJobInfoV1.jobType, testJobInfoV1.properties),
//      10.seconds)
//
//    jobServer ! ScheduledJob(id, testJobInfoV1)
//    expectMsg(JobSucceeded(id))
  }

  // 哥哥我这里是只能看debug日志当测试了
  test("jobServer router as thread-pool") {
    var expectedMsgs: Seq[JobSucceeded] = Seq()
    val count = 20
//    (0 until count).foreach { _ =>
//      val f = sqlService.createJob(testJobInfoV2.job,
//        testJobInfoV2.jobType, testJobInfoV2.properties)
//      val id = Await.result(f, 10.seconds)
//      jobServer ! ScheduledJob(id, testJobInfoV2.copy(priority = count - id))
//      expectedMsgs = expectedMsgs :+ JobSucceeded(id)
//    }
//    expectMsgAllOf(60.seconds, expectedMsgs: _*)
  }

  override def afterAll(): Unit = {
    sqlService.close()
    TestKit.shutdownActorSystem(system)
  }

}

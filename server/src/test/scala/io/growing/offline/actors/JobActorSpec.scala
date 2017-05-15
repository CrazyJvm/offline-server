package io.growing.offline.actors

import akka.actor.{ActorSystem, Props}
import akka.testkit.TestKit
import io.growing.offline.actors.JobScheduler.JobSucceeded
import io.growing.offline.api.TestJobContext

/**
  * Created by king on 12/26/16.
  */
class JobActorSpec(_system: ActorSystem) extends BaseActorSuite(_system) {

  def this() = this(ActorSystem("JobActorSpec"))

  val actor = _system.actorOf(Props(classOf[JobActor], new TestJobContext(null, Map())))

  test("job actor process job") {
//    actor ! ScheduledJob(0, testJobInfoV1)
    expectMsg(JobSucceeded(0))
  }

  test("job actor group") {
    val configString =
      """
        |
      """.stripMargin

  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }
}

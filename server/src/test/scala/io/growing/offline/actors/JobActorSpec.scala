package io.growing.offline.actors

import akka.actor.{ActorSystem, Props}
import akka.testkit.TestKit
import io.growing.offline.actors.JobScheduler.{JobSucceeded, ScheduledJob}
import io.growing.offline.api.TestJobContext
import io.growing.offline.models.{JobGraphNode, ScheduledJobGraphNode}

/**
  * Created by king on 12/26/16.
  */
class JobActorSpec(_system: ActorSystem) extends BaseActorSuite(_system) {

  def this() = this(ActorSystem("JobActorSpec"))

  val actor = _system.actorOf(Props(classOf[JobActor], new TestJobContext(null, Map())))

  // 接收到nextJobs
  val expectedJob = ScheduledJobGraphNode("201705150800", "201705150900",
    JobGraphNode("afterJob", "debug", Map(), 3))

  test("job actor process job") {
    actor ! ScheduledJob(0, scheduledJobGraphNode1)
    expectMsg(JobSucceeded(0))

    expectMsg(expectedJob)
  }

  test("job actor factory") {
    actor ! ScheduledJob(1, scheduledJobGraphNode2)
    expectMsg(JobSucceeded(1))

    expectMsg(expectedJob)
    expectMsg(expectedJob)
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }
}

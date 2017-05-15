package io.growing.offline.actors

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import io.growing.offline.models.{JobGraphNode, JobInfo, ScheduledJobGraphNode}
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike, Matchers}

/**
  * Created by king on 12/26/16.
  */
class BaseActorSuite(_system: ActorSystem) extends TestKit(_system)
  with ImplicitSender with FunSuiteLike with Matchers with BeforeAndAfterAll {

  val scheduledJobGraphNode1 = ScheduledJobGraphNode("201705150800", "201705150900",
    JobGraphNode("io.growing.offline.api.TestSparkJob", "class", Map(), 3))

  val scheduledJobGraphNode2 = ScheduledJobGraphNode("201705150800", "201705150900",
    JobGraphNode("io.growing.offline.api.TestSparkJobFactory", "factory", Map(), 3))
}
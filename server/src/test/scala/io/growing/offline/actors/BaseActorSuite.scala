package io.growing.offline.actors

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import io.growing.offline.models.JobInfo
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike, Matchers}

/**
  * Created by king on 12/26/16.
  */
class BaseActorSuite(_system: ActorSystem) extends TestKit(_system)
  with ImplicitSender with FunSuiteLike with Matchers with BeforeAndAfterAll {

//  val testJobInfoV1: JobInfo = JobInfo("io.growing.offline.api.TestSparkJobV1", "class", Map("succeeded" -> "true"))
//  val testJobInfoV2: JobInfo = JobInfo("io.growing.offline.api.TestSparkJobV2", "class", Map())
}

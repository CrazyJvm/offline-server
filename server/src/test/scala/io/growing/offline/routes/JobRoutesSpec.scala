package io.growing.offline.routes

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, StatusCodes}
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.testkit.{ImplicitSender, TestKitBase}
import org.scalatest.FunSuite

/**
  * Created by king on 1/6/17.
  */
class JobRoutesSpec(_system: ActorSystem) extends FunSuite with ScalatestRouteTest with TestKitBase with ImplicitSender {
  def this() = this(ActorSystem("JobRoutesSpec"))

  private var routers: JobRouters = null

  override def beforeAll(): Unit = {
    routers = new JobRouters(testActor, testActor)
  }

  test("submitter") {
    val jobConfig =
      """
        |{ "name": "job1",
        |  "jobInfo": { "job": "j1", "jobType": "class", "properties": {}, "priority": 0 },
        |  "submitInfo": { "topicAndGroups": [], "interval": 3600 }
        |}
      """.stripMargin

    Post("/submitter", HttpEntity(ContentTypes.`application/json`, jobConfig)) ~> routers.submitterRoute ~> check {
      status === StatusCodes.OK
    }

    Delete("/submitter/job1") ~> routers.submitterRoute ~> check {
      status === StatusCodes.OK
    }
  }

  test("scheduler") {
    val jobInfo =
      """
        |{ "job": "j1", "jobType": "class", "properties": {}, "priority": 0 }
      """.stripMargin

    Post("/scheduler", HttpEntity(ContentTypes.`application/json`, jobInfo)) ~> routers.scheduerRoute ~> check {
      status === StatusCodes.OK
    }

    Put("/scheduler/1") ~> routers.scheduerRoute ~> check {
      status === StatusCodes.OK
    }
  }

}

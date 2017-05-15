package io.growing.offline.routes

import akka.actor.ActorRef
import akka.http.scaladsl.model.{HttpEntity, _}
import akka.http.scaladsl.server.Directives._
import akka.pattern._
import akka.util.Timeout
import io.growing.offline.actors.JobScheduler.{RestartJob, RestartJobFailed, RestartJobSucceeded}
import io.growing.offline.actors.Submitter.{SubmitJobAdd, SubmitJobBlock, SubmitJobRemove, SubmitJobResume}
import io.growing.offline.models.{JacksonSupport, JobConfig, JobInfo}

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
  * Created by king on 1/6/17.
  */
class JobRouters(submitter: ActorRef, scheduler: ActorRef)
                (implicit val executionContext: ExecutionContextExecutor) extends JacksonSupport {
  implicit val timeout = Timeout(10.seconds)

  def routes = indexRoute ~ submitterRoute ~ scheduerRoute

  val indexRoute = path("index") {
    get {
      okResponse("<h1>Say hello to akka-http</h1>")
//      complete(HttpEntity(ContentTypes.`text/plain(UTF-8)`, "<h1>Say hello to akka-http</h1>"))
    }
  }

  val submitterRoute = pathPrefix("submitter") {
    post {
      entity(as[JobConfig]) { jobConfig =>
        // submit new job
        submitter ! SubmitJobAdd(jobConfig)
        complete(HttpResponse(status = StatusCodes.OK,
          entity = HttpEntity(ContentTypes.`text/plain(UTF-8)`, s"Job Submitted. ${jobConfig.name}")))
      }
    } ~ delete {
      path(Segment) { submit_name =>
        // remove a job
        submitter ! SubmitJobRemove(submit_name)
        complete(HttpResponse(status = StatusCodes.OK,
          entity = HttpEntity(ContentTypes.`text/plain(UTF-8)`, s"Job Removed. $submit_name")))
      }
    } ~ get {
      (pathPrefix("block") & path(Segment)) { submit_name =>
        submitter ! SubmitJobBlock(submit_name)
        okResponse(s"$submit_name blocked.")
      } ~ (pathPrefix("resume") & path(Segment)) { submit_name =>
        submitter ! SubmitJobResume(submit_name)
        okResponse(s"$submit_name resumed.")
      }
    }
  }

  val scheduerRoute = pathPrefix("scheduler") {
    (post & entity(as[JobInfo])) { jobInfo =>
      // run new Job
//      scheduler ! ScheduledJobGraphNode(jobInfo)
      complete(HttpResponse(status = StatusCodes.OK,
        entity = HttpEntity(ContentTypes.`text/plain(UTF-8)`, s"Job Scheduled. $jobInfo")))
    } ~ (put & path(IntNumber)) { id =>
      // restart some job
      onComplete(scheduler ? RestartJob(id)) {
        case Success(res) => res match {
          case RestartJobSucceeded => okResponse(s"Job $id restarted.")
          case RestartJobFailed(_id, reason) => badResponse(reason)
          case _ => badResponse(s"unknown error")
        }
        case Failure(ex) => badResponse(ex.toString)
      }
    }
  }

  private def okResponse(message: String) = {
    complete(HttpResponse(status = StatusCodes.OK,
      entity = HttpEntity(ContentTypes.`text/plain(UTF-8)`, message)))
  }

  private def badResponse(reason: String) = {
    complete(HttpResponse(status = StatusCodes.BadRequest,
      entity = HttpEntity(ContentTypes.`text/plain(UTF-8)`, reason)))
  }

}

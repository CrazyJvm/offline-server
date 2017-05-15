package io.growing.offline

import java.time.ZoneId

import akka.actor.{ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import com.typesafe.config.ConfigFactory
import io.growing.offline.actors.JobScheduler.StartScheduler
import io.growing.offline.actors.Submitter.StartSubmitter
import io.growing.offline.actors.{JobScheduler, Submitter}
import io.growing.offline.models.JobConfig
import io.growing.offline.routes.JobRouters
import io.growing.offline.services.{KafkaService, SqlService}
import io.growing.offline.utils.ConfigUtils

/**
  * Created by king on 12/20/16.
  */
object Server {

  def main(args: Array[String]) {
    val config = if (args.length == 1) {
      ConfigFactory.load(args.head)
    } else {
      ConfigFactory.load()
    }

    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()

    implicit val executionContext = system.dispatcher

    implicit val timeZone: ZoneId = ConfigUtils.timeZone(config)

    val sqlService = new SqlService(config)
    val kafkaService = new KafkaService(ConfigUtils.parseKafkaParams(config))

    val jobConfigs: Set[JobConfig] = ConfigUtils.parseJobConfigs(config)

    val submitter = system.actorOf(Props(classOf[Submitter],
      config.getConfig("app.submitter"), jobConfigs, sqlService, kafkaService), "submitter")
    val scheduler = system.actorOf(Props(classOf[JobScheduler],
      config.getConfig("app.scheduler"), jobConfigs, sqlService), "scheduler")

    submitter ! StartSubmitter(scheduler)
    scheduler ! StartScheduler(submitter)

    val router = new JobRouters(submitter, scheduler)

    val bindingFuture = Http().bindAndHandle(router.routes, "0.0.0.0", 18080)

//    StdIn.readLine() // let it run until user presses return
//    bindingFuture
//      .flatMap(_.unbind()) // trigger unbinding from the port
//      .onComplete(_ => system.terminate()) // and shutdown when done
  }


}

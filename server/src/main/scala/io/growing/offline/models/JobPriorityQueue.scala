package io.growing.offline.models

import akka.actor.{ActorSystem, PoisonPill}
import akka.dispatch.{PriorityGenerator, UnboundedPriorityMailbox}
import com.typesafe.config.Config
import io.growing.offline.actors.JobScheduler.ScheduledJob

/**
  * Created by king on 12/24/16.
  */
class JobPriorityQueue(settings: ActorSystem.Settings, config: Config) extends
  UnboundedPriorityMailbox(PriorityGenerator {
  case ScheduledJob(_, scheduledJobGraphNode) =>
    scheduledJobGraphNode.jobGraphNode.priority
  case PoisonPill => 0
  case _ => 1
})

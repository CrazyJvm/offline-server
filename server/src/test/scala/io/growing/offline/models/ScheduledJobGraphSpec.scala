package io.growing.offline.models

import com.typesafe.config.ConfigFactory
import io.growing.offline.actors.JobScheduler.ScheduledJob
import io.growing.offline.utils.ConfigUtils
import org.scalatest.{FunSuite, Matchers}

/**
  * Created by king on 17-5-15.
  */
class ScheduledJobGraphSpec extends FunSuite with Matchers {

  test("ScheduledJobGraph scheduleJobs") {
    val config = ConfigFactory.load("application-test")
    val jobConfigs = ConfigUtils.parseJobConfigs(config).map(j => j.name -> j).toMap
    val jobInfo1 = jobConfigs("test-hourly-jobs1").jobInfo
    val scheduledJobGraph = ScheduledJobGraph("test-hourly-jobs1", "201705150800", "201705150900",
      jobInfo1.jobGraph, jobInfo1.terminalJob)

    val registeredJobs = jobInfo1.jobGraph.jobNodesIndex.map(j => j._1 -> (j._2 + 1))

    scheduledJobGraph.initial(registeredJobs)
    var scheduledJobs = scheduledJobGraph.scheduleJobs()
//    println(scheduledJobs)
    val expectedResult1 = Array(ScheduledJob(5, ScheduledJobGraphNode("201705150800", "201705150900",
      JobGraphNode("io.growing.test.TestJob0", "debug", Map("typ" -> "hour"), 3))))
    scheduledJobs should equal(expectedResult1)

    scheduledJobGraph.updateMark("io.growing.test.TestJob0")
    scheduledJobs = scheduledJobGraph.scheduleJobs()
//    println(scheduledJobs)
    val expectedResult2 = Array(ScheduledJob(1, ScheduledJobGraphNode("201705150800", "201705150900",
      JobGraphNode("io.growing.test.TestJob1", "debug", Map("typ" -> "hour"), 3))),
      ScheduledJob(2, ScheduledJobGraphNode("201705150800", "201705150900",
        JobGraphNode("io.growing.test.TestJob2", "debug", Map("typ" -> "hour"), 3))))
    scheduledJobs should equal(expectedResult2)

    scheduledJobGraph.updateMark("io.growing.test.TestJob1")
    scheduledJobs = scheduledJobGraph.scheduleJobs()
//    println(scheduledJobs)
    scheduledJobs shouldBe empty

    scheduledJobGraph.jobGraphFinished() should be(false)
    scheduledJobGraph.terminalJobFinished() should be(false)

    scheduledJobGraph.updateMark("io.growing.test.TestJob2")
    scheduledJobGraph.terminalJobFinished() should be(true)

    scheduledJobs = scheduledJobGraph.scheduleJobs()
//    println(scheduledJobs)
    val expectedResult3 = Array(ScheduledJob(3, ScheduledJobGraphNode("201705150800", "201705150900",
      JobGraphNode("io.growing.test.TestJob3", "debug", Map("typ" -> "hour"), 3))),
      ScheduledJob(4, ScheduledJobGraphNode("201705150800", "201705150900",
        JobGraphNode("io.growing.test.TestJob4", "debug", Map("typ" -> "hour"), 3))))
    scheduledJobs should equal(expectedResult3)
  }

}

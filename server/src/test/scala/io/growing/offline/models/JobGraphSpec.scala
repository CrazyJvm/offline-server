package io.growing.offline.models

import com.typesafe.config.ConfigFactory
import io.growing.offline.utils.ConfigUtils
import org.scalatest.{FunSuite, Matchers}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by king on 17-5-15.
  */
class JobGraphSpec extends FunSuite with Matchers {

  test("job graph initial matrix correctly") {
    val config = ConfigFactory.load("application-test")
    val jobConfigs = ConfigUtils.parseJobConfigs(config).map(j => j.name -> j).toMap
    val jobGraph1 = jobConfigs("test-hourly-jobs1").jobInfo.jobGraph
    println(jobGraph1.matrix.toSeq)
    println(jobGraph1.jobNodesIndex.toSeq)

    val expectedMatrix = Array(ArrayBuffer(4), ArrayBuffer(4), ArrayBuffer(0, 1), ArrayBuffer(1), ArrayBuffer())
    val expectedNodesIndex = Map(("io.growing.test.TestJob1",0),
      ("io.growing.test.TestJob2",1), ("io.growing.test.TestJob3",2),
      ("io.growing.test.TestJob4",3), ("io.growing.test.TestJob0",4))

    jobGraph1.matrix should equal(expectedMatrix)
    jobGraph1.jobNodesIndex should equal(expectedNodesIndex)
  }

}

package io.growing.offline.models

import org.scalatest.FunSuite

/**
  * Created by king on 1/5/17.
  */
class JobConfigSpec extends FunSuite {

  test("jobConfig checkJob") {
    val tg = TopicAndGroup("t1", "g1")
    val tm = System.currentTimeMillis()
//    val jobConfig = JobConfig("job", JobInfo("job1", "class", Map()), SubmitInfo("hourly", Seq(tg)))
//    var res = jobConfig.checkJob(Map(tg -> List()), tm)
//    assert(!res)
//
//    res = jobConfig.checkJob(Map(tg -> List(tm + 10000)), tm)
//    assert(res)
//
//    res = jobConfig.checkJob(Map(tg -> List(tm - 10000)), tm)
//    assert(!res)
  }

}

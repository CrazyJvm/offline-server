package io.growing.offline.api

import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

/**
  * Created by king on 12/30/16.
  * 用于测试BaseJobContext内的utils方法
  */
class JobContextSpec extends FunSuite with Matchers with BeforeAndAfterAll {

  var jobContext: BaseJobContext = null

  override def beforeAll(): Unit = {
    jobContext = new JobContext(Map(
      "spark.master" -> "local[2]",
      "spark.app.name" -> "local-JobContext",
      "timezone" -> "Asia/Shanghai"
    ))
  }

  override def afterAll(): Unit = {
    jobContext.stop()
  }



}

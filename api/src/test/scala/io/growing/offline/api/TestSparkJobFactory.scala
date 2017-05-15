package io.growing.offline.api

/**
  * Created by king on 17-5-15.
  */
class TestSparkJobFactory(jobContext: BaseJobContext, conf: Map[String, String]) extends SparkJobFactory(jobContext, conf) {
  override def generateJobs(): Seq[NextJobInfo] = {
    val properties = conf.filterKeys(!Set("startPos", "stopPos").contains(_))

    Seq(
      NextJobInfo("io.growing.offline.api.TestSparkJob", "class", startPos, stopPos, properties ++ Map("from" -> "factory1")),
      NextJobInfo("io.growing.offline.api.TestSparkJob", "class", startPos, stopPos, properties ++ Map("from" -> "factory2"))
    )
  }
}

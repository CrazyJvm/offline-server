package io.growing.offline.api

/**
  * Created by king on 5/9/17.
  */
abstract class SparkJobFactory(jobContext: BaseJobContext, conf: Map[String, String]) extends Logging {
  protected lazy val startPos: String = conf.getOrElse("startPos",
    throw new Exception(s"missing startPos in ${getClass.getSimpleName}"))
  protected lazy val stopPos: String = conf.getOrElse("stopPos",
    throw new Exception(s"missing stopPos in ${getClass.getSimpleName}"))

  def generateJobs(): Seq[NextJobInfo]

}

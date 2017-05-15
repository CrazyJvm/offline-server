package io.growing.offline.api

/**
  * Created by king on 12/24/16.
  */
case class SparkJobResult(succeeded: Boolean,
                          nextJobs: Seq[NextJobInfo] = Seq(),
                          reason: Option[Exception] = None) {
  override def toString: String = {
    if (succeeded) {
      s"job succeeded. and next jobs: $nextJobs"
    } else {
      s"job failed. reason: ${reason.get}"
    }
  }
}
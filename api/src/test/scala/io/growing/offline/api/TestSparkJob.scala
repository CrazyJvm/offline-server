package io.growing.offline.api

/**
  * Created by king on 17-5-15.
  */
class TestSparkJob(jobContext: BaseJobContext, conf: Map[String, String]) extends SparkJob(jobContext, conf) {
  /**
    * 定义Job的计算过程, 预先定义不同类型的计算任务
    * 单独定义的作用主要是为了便于测试
    *
    * @return DataFrame或者RDD
    */
  override def compute(): Any = {
    logger.info(s"running TestSparkJob startPos: $startPos, stopPos: $stopPos, conf: $conf.")
  }

  override def run(): SparkJobResult = {
    compute()
    SparkJobResult(succeeded = true, nextJobs = after())
  }

  override def after(): Seq[NextJobInfo] = {
    Seq(NextJobInfo("afterJob", "debug", startPos, stopPos, Map(), 3))
  }
}
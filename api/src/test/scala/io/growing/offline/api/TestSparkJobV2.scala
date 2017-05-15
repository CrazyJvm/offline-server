package io.growing.offline.api

/**
  * Created by king on 12/26/16.
  * 用于测试JobServer的router
  */
class TestSparkJobV2(jobContext: BaseJobContext, properties: Map[String, String])
  extends SparkJob(jobContext, properties) {
  /**
    * 定义Job的计算过程, 预先定义不同类型的计算任务
    * 单独定义的作用主要是为了便于测试
    *
    * @return DataFrame或者RDD
    */
  override def compute(): Any = {}

  override def run(): SparkJobResult = {
    Thread.sleep(2000)
    SparkJobResult(succeeded = true)
  }
}

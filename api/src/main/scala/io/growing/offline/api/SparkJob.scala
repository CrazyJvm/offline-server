package io.growing.offline.api

import io.growing.offline.store.Store
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable

/**
  * Created by king on 12/24/16.
  * 所有的计算结果都会存储到HDFS上
  *
  * 数据计算的测试必须能够获取计算结果的输出
  * 所以,必须使用compute()方法,禁止override run()
  */
abstract class SparkJob(jobContext: BaseJobContext, conf: Map[String, String]) extends Store with Logging {
  protected lazy val startPos: String = conf.getOrElse("startPos",
    throw new Exception(s"missing startPos in ${getClass.getSimpleName}"))
  protected lazy val stopPos: String = conf.getOrElse("stopPos",
    throw new Exception(s"missing stopPos in ${getClass.getSimpleName}"))
  protected implicit lazy val timeZone: String = jobContext.timeZone

  private val originLocalProperty: mutable.HashMap[String, String] = mutable.HashMap.empty
  private val sc = jobContext.session.sparkContext
  private val descriptionKeys = jobContext.descriptionKeys ++ Set("startPos", "stopPos")

  def run(): SparkJobResult = {
    try {
      preStart()
      setLocalProperty(conf)
      compute() match {
        case df: DataFrame @unchecked =>
          store(df)
        case rdd: RDD[Row] @unchecked =>
          store(rdd)
        case _ =>
          // 这种情况下,一般job可以创建下一个时间点的表,执行清理数据等任务
          // 也可以用于测试
      }
      SparkJobResult(succeeded = true, nextJobs = after())
    } catch {
      case e: Exception =>
        SparkJobResult(succeeded = false, reason = Some(e))
    } finally {
      resetLocalProperty()
      try {
        postStop()
      } catch {
        case e: Exception =>
          logger.error("can not process postStop.")
      }
    }
  }

  /**
    * 定义Job的计算过程, 预先定义不同类型的计算任务
    * 单独定义的作用主要是为了便于测试
    * @return DataFrame或者RDD
    */
  def compute(): Any

  /**
    * 预留preStart 与 postStop 接口
    */

  def preStart(): Unit = {}

  def postStop(): Unit = {}


  /**
    * 若是该任务有后续的任务需要执行,则定义在after内
    * @return
    */
  def after(): Seq[NextJobInfo] = {
      Nil
  }

  private def setLocalProperty(configs: Map[String, String]): Unit = {
    // 保留预先的localProperty
    val sparkConfs = configs.filter(_._1.startsWith("spark."))
    sparkConfs.keySet.foreach(key =>
      originLocalProperty += key -> sc.getLocalProperty(key))

    sparkConfs.foreach(c =>
      sc.setLocalProperty(c._1, c._2)
    )

    // 设置spark.jobGroup.id
    val groupKey = "spark.jobGroup.id"
    originLocalProperty += groupKey -> sc.getLocalProperty(groupKey)
    sc.setLocalProperty(groupKey, this.getClass.getSimpleName)

    // 设置spark.job.description, 默认为其properties参数
    val descKey = "spark.job.description"
    // 非spark的配置项
    val desc = configs.filter(c => descriptionKeys.exists(c._1.startsWith))
      .map(m => s"${m._1} = ${m._2}")
      .mkString(", ")
    originLocalProperty += descKey -> sc.getLocalProperty(descKey)
    sc.setLocalProperty(descKey, desc)
  }

  /**
    * called after setLocalProperty
    */
  private def resetLocalProperty(): Unit = {
    originLocalProperty.foreach(c =>
      jobContext.session.sparkContext.setLocalProperty(c._1, c._2)
    )
    originLocalProperty.clear()
  }

}

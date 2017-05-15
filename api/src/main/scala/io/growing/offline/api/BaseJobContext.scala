package io.growing.offline.api

import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{LocalDateTime, ZoneId}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by king on 12/24/16.
  * spark.app.name
  * spark.master
  * spark.submit.deployMode
  *
  */
trait BaseJobContext {
  val session: SparkSession

  def contextConf: Map[String, String]

  def timeZone: String = contextConf.getOrElse("timezone", "Asia/Shanghai")

  def zkUrl: String = contextConf.getOrElse("zkUrl", "localhost:2181")

  def outputPathPrefix: String = contextConf.getOrElse("outputPathPrefix", "file:///tmp")

  def descriptionKeys: Set[String] = contextConf.getOrElse("descriptionKeys", "")
    .split(",").map(_.trim).toSet

  /** utils 方法 **/
  def sql(sqlText: String): DataFrame = {
    session.sql(sqlText)
  }

  def stop(): Unit = {
    session.stop()
  }

}

class JobContext(val conf: Map[String, String]) extends BaseJobContext {
  override val session: SparkSession = {
    val sparkConf = new SparkConf()
    conf.filter(_._1.startsWith("spark."))
      .foreach(c => sparkConf.set(c._1, c._2))

    val _session = SparkSession.builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()

    conf.filter(_._1.startsWith("hadoopConf.")).foreach{ c =>
      _session.sparkContext.hadoopConfiguration.set(c._1.substring(11), c._2)
    }

    _session
  }

  override def timeZone: String = conf.getOrElse("timezone", "Asia/Shanghai")

  override def contextConf = conf

  // 自动注册hbase表
  if (conf.contains("phoenix.tables")) {
    conf("phoenix.tables").split(",").map(_.trim).foreach ( table =>
      session.read.format("org.apache.phoenix.spark")
        .options(Map("table" -> table, "zkUrl" -> zkUrl))
        .load().createOrReplaceTempView(table)
    )
  }

  // 注册jdbc表
  if (conf.contains("jdbc.tables")) {
    conf("jdbc.tables").split(",").map(_.trim).foreach { table =>
      val prefix = s"database.$table."
      val tableConf = conf.filter(_._1.startsWith(prefix)).map(m =>
        m._1.substring(prefix.length) -> m._2
      ) ++ Map("dbtable" -> table)
      session.read.format("jdbc").options(tableConf)
        .load().createOrReplaceTempView(table)
    }
  }

}

class TestJobContext(_session: SparkSession, conf: Map[String, String] = Map()) extends BaseJobContext {
  override val session: SparkSession = _session

  override def timeZone: String = conf.getOrElse("timeZone", "Asia/Shanghai")

  override def contextConf: Map[String, String] = conf

  override def stop(): Unit = {
//    session.stop()
  }
}
package io.growing.offline

import java.nio.charset.StandardCharsets

import com.typesafe.config.ConfigFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.test.TestHiveContext
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.collection.JavaConverters._
import scala.io.Source

/**
  * Created by king on 12/29/16.
  */
trait SparkTestBase extends FunSuite with Matchers with BeforeAndAfterAll {
  // 有一些配置必须在sparkConf
  private val configString: String =
    """
      |spark {
      |   es {
      |       nodes: "cnem0"
      |       port: 9200
      |       index.auto.create: "true"
      |       mapping.id: "_id"
      |       batch.size.entries: 20000
      |       batch.size.bytes: 16mb
      |       circle.index.prefix: "circle-"
      |    }
      |}
    """.stripMargin

  @transient val hiveContext = {
    val _conf = new SparkConf()
      .set("spark.sql.test", "")
      .set("spark.ui.enabled", "false")
      .set("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
      .set("hive.exec.dynamic.partition.mode", "nonstrict")
      .setMaster("local[2]")
      .setAppName("TestHiveContext")

    ConfigFactory.parseString(configString).entrySet().asScala.foreach { e =>
      _conf.set(e.getKey, e.getValue.unwrapped().toString)
    }

    new TestHiveContext(new SparkContext(_conf), loadTestTables = false)
  }

  def session: SparkSession = hiveContext.sparkSession

  def checkAnswer(runningAnswer: DataFrame, expectedAnswer: DataFrame): Unit = {
    checkAnswer(runningAnswer.collect(), expectedAnswer.collect())
  }

  def checkAnswer(runningAnswer: Seq[Row], expectedAnswer: Seq[Row]): Unit = {
    def prepareRow(row: Row): Row = {
      Row.fromSeq(row.toSeq.map {
        case null => null
        // Convert array to Seq for easy equality check.
        case b: Array[_] => b.toSeq
        case r: Row => prepareRow(r)
        case o => o
      })
    }

    runningAnswer.map(prepareRow) should contain theSameElementsAs
      expectedAnswer.map(prepareRow)
  }

  def checkAnswer(runningAnswer: RDD[Row], expectedAnswer: DataFrame): Unit = {
    checkAnswer(runningAnswer.collect(), expectedAnswer.collect())
  }

  def createDatabase(database: String): Unit = {
    session.sql(s"create database if not exists $database")
  }

  /**
    * sqlPath must contain the tableName
    *
    * @param sqlPath the sqlPath in classpath
    * @param dataPath dataPath in classpath
    * @param partitions if the table has partitions
    * @return
    */
  def loadTestDataframe(sqlPath: String, dataPath: String,
                        partitions: String = "", isHbase: Boolean = false): DataFrame = {
    val sql = Source.fromInputStream(getClass.getResourceAsStream("/" + sqlPath))
      .getLines().mkString

    if (isHbase) {
      hiveContext.sql(trimDDLSql(transformSql(sql)))
    } else {
      hiveContext.sql(trimDDLSql(sql))
    }

    val tableName = sqlPath.split("/").last.dropRight(4)

    // 插入测试数据
    val loadSql =
      s"""
         |LOAD DATA LOCAL INPATH '${quoteTestFilePath(dataPath)}'
         |OVERWRITE INTO TABLE $tableName $partitions
      """.stripMargin
    hiveContext.sql(loadSql)
  }

  private def quoteTestFilePath(dataPath: String): String = {
    Thread.currentThread().getContextClassLoader.getResource(dataPath).getPath
  }

  private def trimDDLSql(sql: String): String = {
    val index = sql.toLowerCase.indexOf("stored")
    val res = if (index == -1) sql else sql.substring(0, index)

    s"""
      |$res
      |ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' NULL DEFINED AS ''
      |STORED AS TEXTFILE
      |TBLPROPERTIES ('serialization.null.format'='')
    """.stripMargin
  }

  /**
    * 将 phoenix ddl sql 转化为 spark hive ddl sql
    * 这样在测试的时候就不需要写两套代码
    */
  private def transformSql(sql: String): String = {
    val constraintIndex = sql.toLowerCase.indexOf("constraint")
    val lastComma = sql.lastIndexOf(',', constraintIndex)
    val cleanSql = sql.substring(0, lastComma)

    // 将所有的varchar转换为string
    cleanSql.replaceAll("varchar", "string").replaceAll("not null", "") + ")"
  }

  def toBytes(str: String):  Array[Byte] = {
    str.getBytes(StandardCharsets.UTF_8)
  }

  override def afterAll(): Unit = {
    hiveContext.reset()
    session.stop()
  }

}

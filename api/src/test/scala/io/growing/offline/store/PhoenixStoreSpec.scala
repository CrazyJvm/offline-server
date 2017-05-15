package io.growing.offline.store

import java.nio.charset.StandardCharsets
import java.sql.{Connection, DriverManager}

import io.growing.offline.SparkTestBase
import org.apache.hadoop.hbase.HConstants
import org.apache.phoenix.end2end.BaseHBaseManagedTimeIT
import org.apache.phoenix.query.BaseTest
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.phoenix.spark._
import org.apache.spark.sql.types._

/**
  * Created by king on 1/9/17.
  */
object PhoenixStoreSpecHelper extends BaseHBaseManagedTimeIT {
  def getTestClusterConfig = BaseHBaseManagedTimeIT.getTestClusterConfig
  def doSetup = BaseHBaseManagedTimeIT.doSetup()
  def doTeardown = BaseHBaseManagedTimeIT.doTeardown()
  def getUrl = BaseTest.getUrl
}

object PhoenixStoreDemo extends PhoenixStore {
  override val zkUrl: String = "localhost:2181"
  override val outputPathPrefix: String = "file:///tmp/hdfs"
}

class PhoenixStoreSpec extends SparkTestBase {
  var conn: Connection = null
  private val table = "table1"
  private val schema = StructType(Seq(StructField("id", LongType),
    StructField("colb", BinaryType),
    StructField("cola", StringType)))
  private val zkUrl = PhoenixStoreDemo.zkUrl

  override def beforeAll(): Unit = {
    super.beforeAll()
    PhoenixStoreSpecHelper.doSetup

    conn = DriverManager.getConnection(PhoenixStoreSpecHelper.getUrl)
    conn.setAutoCommit(true)

    val stmt = conn.createStatement()
    stmt.execute(s"CREATE TABLE $table (id BIGINT NOT NULL PRIMARY KEY, cola VARCHAR, colb VARBINARY)")

    conn.commit()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    conn.close()
    PhoenixStoreSpecHelper.doTeardown
  }

  test("PhoenixStore store2phoenix support any seq column") {
    val data = Row(1L, toBytes("colb1"), "cola1") ::
      Row(2L, toBytes("colb2"), "cola2") :: Nil
    val df = session.createDataFrame(
      session.sparkContext.makeRDD(data), schema)

    PhoenixStoreDemo.store2phoenix(df, table = "table1")

    session.sqlContext.phoenixTableAsDataFrame("table1",
      Seq("ID", "COLA", "COLB"), zkUrl = Some(zkUrl))

    session.read.format("org.apache.phoenix.spark")
      .options(Map("table" -> table, "zkUrl" -> zkUrl))
      .load().createOrReplaceTempView(table)
    val runningResult = session.sql(s"select id, colb, cola from $table")

    checkAnswer(runningResult.collect(), data)

    reset(table)
  }

  test("PhoenixStore bulkStore2phoenix support any seq column") {
    val data = Row(3L, toBytes("colb3"), "cola3") ::
      Row(4L, toBytes("colb4"), "cola4") :: Nil

    val df = session.createDataFrame(
      session.sparkContext.makeRDD(data), schema)

    PhoenixStoreDemo.bulkStore2phoenix(df, table)

    session.read.format("org.apache.phoenix.spark")
      .options(Map("table" -> table, "zkUrl" -> zkUrl))
      .load().createOrReplaceTempView(table)
    val runningResult = session.sql(s"select id, colb, cola from $table")

    checkAnswer(runningResult.collect(), data)

    reset(table)
  }

  test("PhoenixStore bulkStore2phoenix without schema") {
    val data = Row(3L, "cola3", toBytes("colb3")) ::
      Row(4L, "cola4", toBytes("colb4")) :: Nil

    PhoenixStoreDemo.bulkStore2phoenix(session.sparkContext.makeRDD(data), table)

    session.read.format("org.apache.phoenix.spark")
      .options(Map("table" -> table, "zkUrl" -> zkUrl))
      .load().createOrReplaceTempView(table)
    val runningResult = session.sql(s"select id, cola, colb from $table")

    checkAnswer(runningResult.collect(), data)

    reset(table)
  }

  test("PhoenixStore tableSchema") {
    PhoenixStoreDemo.phoenixTableSchema(table) should equal(schema)
  }

  test("salt_bucket table schema") {
    conn.createStatement()
      .execute("CREATE TABLE table2 (id BIGINT NOT NULL PRIMARY KEY, col VARCHAR) SALT_BUCKETS=4")
    val table2Schema = StructType(Seq(StructField( "id", LongType), StructField("col", StringType)))
    PhoenixStoreDemo.phoenixTableSchema("table2") should equal(table2Schema)
  }

  private def reset(tableName: String): Unit = {
    conn.createStatement().execute(s"delete from $tableName")
  }
}

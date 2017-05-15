package io.growing.offline.store

import java.net.URLEncoder
import java.sql.DriverManager
import java.util

import io.growing.offline.api.Logging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.KeyValue.KVComparator
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.RawComparator
import org.apache.hadoop.mapreduce.OutputFormat
import org.apache.phoenix.jdbc.PhoenixConnection
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil
import org.apache.phoenix.schema.types.{PDouble, PFloat, PUnsignedDouble, PUnsignedFloat, _}
import org.apache.phoenix.spark._
import org.apache.phoenix.util.{ColumnInfo, PhoenixRuntime, QueryUtil}
import org.apache.spark.Partitioner
import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.JavaConverters._

/**
  * Created by king on 1/7/17.
  */
trait PhoenixStore extends HiveStore {
  val zkUrl: String
  // 若是使用 bulkStore2phoenix, 必须设置
  val outputPathPrefix: String = null

  /**
    * 从 phoenix中取出对应需要存储数据的schema
    * @param table 目标表名
    * @return
    */
  def phoenixTableSchema(table: String): StructType = {
    def phoenixTypeToCatalystType(phoenixType: PDataType[_]): DataType = phoenixType match {
      case t if t.isInstanceOf[PVarchar] || t.isInstanceOf[PChar] => StringType
      case t if t.isInstanceOf[PLong] || t.isInstanceOf[PUnsignedLong] => LongType
      case t if t.isInstanceOf[PInteger] || t.isInstanceOf[PUnsignedInt] => IntegerType
      case t if t.isInstanceOf[PFloat] || t.isInstanceOf[PUnsignedFloat] => FloatType
      case t if t.isInstanceOf[PDouble] || t.isInstanceOf[PUnsignedDouble] => DoubleType
      case t if t.isInstanceOf[PVarbinary] || t.isInstanceOf[PBinary] => BinaryType
      case t => throw new Exception(s"Unsupported phoenix type $t")
    }

    val columnInfoList = getColumnsInfo(table)

    val structFields = columnInfoList.map(ci => {
      val structType = phoenixTypeToCatalystType(ci.getPDataType)
      StructField(ci.getDisplayName.toLowerCase, structType)
    })

    StructType(structFields)
  }

  /**
    * rdd 必须与 table 内的column保持一致
    */
  def store2phoenix(session: SparkSession, rdd: RDD[Row], table: String): Unit = {
    val schema = phoenixTableSchema(table)
    session.createDataFrame(rdd, schema).saveToPhoenix(
      table, conf = session.sparkContext.hadoopConfiguration, Some(zkUrl))
  }

  def store2phoenix(df: DataFrame, table: String): Unit = {
    val fields = df.schema.fieldNames.map(m => s"$m as ${m.toUpperCase}")
    df.selectExpr(fields: _*)
      .saveToPhoenix(table, conf = df.sparkSession.sparkContext.hadoopConfiguration, Some(zkUrl))
  }

  /**
    * 在调用该方法之前需要创建对应的buffer表
    */
  def bufferStore2phoenix(session: SparkSession, rdd: RDD[Row], table: String, bufferPos: String): Unit = {
    val schema = phoenixTableSchema(table)
    val df = session.createDataFrame(rdd, schema)
    bufferStore2phoenix(df, table, bufferPos)
  }

  def bufferStore2phoenix(df: DataFrame, table: String, bufferPos: String): Unit = {
    store2hive(df, s"${table}_BUFFER", Seq(("timepos", Some(bufferPos))))
    store2phoenix(df, table)
  }

  def bulkStore2phoenix(df: DataFrame, table: String): Unit = {
    require(outputPathPrefix != null, "outputPathPrefix not defined")

    bulkStore2phoenix(df.rdd, df.schema, table)
  }

  def bulkStore2phoenix(rdd: RDD[Row], table: String): Unit = {
    require(outputPathPrefix != null, "outputPathPrefix not defined")

    val cols = getColumnsInfo(table).map(_.toString)
    bulkStore2phoenix(rdd, table, cols)
  }

  def bulkStore2phoenix(srcRdd: RDD[Row], schema: StructType, table: String): Unit = {
    require(outputPathPrefix != null, "outputPathPrefix not defined")

    val cols = schema.map {
      case StructField(name, StringType, _, _) => s"VARCHAR:${name.toUpperCase}"
      case StructField(name, LongType, _, _) => s"BIGINT:${name.toUpperCase}"
      case StructField(name, IntegerType, _, _) => s"INTEGER:${name.toUpperCase}"
      case StructField(name, DoubleType, _, _) => s"DOUBLE:${name.toUpperCase}"
      case StructField(name, BinaryType, _, _) => s"VARBINARY:${name.toUpperCase}"
      case StructField(name, t, _, _) => throw new Exception(s"not support [$t] yet")
    }

    bulkStore2phoenix(srcRdd, table, cols)
  }

  def bulkStore2phoenix(srcRdd: RDD[Row], table: String, cols: Seq[String]): Unit = {
    require(outputPathPrefix != null, "outputPathPrefix not defined")

    val bulkLoadTooL = new BulkLoadTooL(zkUrl, outputPathPrefix)
    bulkLoadTooL.process(srcRdd, table, cols)
  }



  private def getColumnsInfo(table: String): Seq[ColumnInfo] = {
    Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
    val conn =
      DriverManager.getConnection(s"jdbc:phoenix:$zkUrl")
    val columnInfoList = PhoenixRuntime.generateColumnInfo(conn, table, null)
      .asScala
    conn.close()
    columnInfoList
  }

}

class BulkLoadTooL(zkUrl: String, outputPathPrefix: String) extends Serializable {
  def process(srcRdd: RDD[Row], table: String, cols: Seq[String]): Unit = {

    val conf = new Configuration(srcRdd.sparkContext.hadoopConfiguration)
    val outputPath = outputPathPrefix + s"/${table}_${System.currentTimeMillis()}"

    conf.setClass("mapreduce.job.outputformat.class", classOf[HFileOutputFormat2],
      classOf[OutputFormat[ImmutableBytesWritable, KeyValue]])
    conf.set("hbase.zookeeper.quorum", zkUrl)
    conf.set("mapreduce.output.fileoutputformat.outputdir", outputPath)

    FileSystem.get(conf).delete(new Path(outputPath), true)

    val htable = new HTable(conf, table.toUpperCase)
    val startKeys = htable.getStartKeys

    val compressValue = new StringBuilder
    var i = 0
    htable.getTableDescriptor.getFamilies.asScala.foreach { f =>
      if (i > 0) compressValue.append('&')
      compressValue.append(URLEncoder.encode(f.getNameAsString, "UTF-8"))
      compressValue.append('=')
      compressValue.append(URLEncoder.encode(f.getCompression.getName, "UTF-8"))
      i += 1
    }
    conf.set("hbase.hfileoutputformat.families.compression", compressValue.toString)

    val kvRdd: RDD[(SerKeyValue, Int)] = srcRdd.mapPartitions { iter =>
      Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
      val conn =
        DriverManager.getConnection(s"jdbc:phoenix:$zkUrl").asInstanceOf[PhoenixConnection]

      val exec = new RowUpsertExecutor(conn, table, cols.map(ColumnInfo.fromString))
      val processor = PhoenixConfigurationUtil.loadPreUpsertProcessor(new Configuration())
      val rst = new Iterator[(SerKeyValue, Int)] {
        var current: Iterator[(SerKeyValue, Int)] = Iterator.empty

        override final def hasNext: Boolean = {
          if (current.hasNext) true
          else {
            var i = 0
            while (iter.hasNext && i < 200) {
              i += 1
              exec.execute(iter.next)
            }
            current =
              PhoenixRuntime.getUncommittedDataIterator(conn, true).asScala.flatMap { x =>
                processor.preUpsert(x.getFirst, x.getSecond).asScala.map { kv =>
                  (SerKeyValue(kv.getValueArray, kv.getOffset, kv.getLength), 0)
                }
              }
            conn.rollback()
            current.hasNext
          }
        }

        override final def next(): (SerKeyValue, Int) = current.next
      }
      conn.close()
      rst
    }

    val hfile =
      new ShuffledRDD[SerKeyValue, Int, Int](kvRdd, new HfilePartitioner(startKeys)).setKeyOrdering(order).map {
        case (k, v) =>
          val kv = k.toKeyValue
          (new ImmutableBytesWritable(kv.getRowArray, kv.getRowOffset, kv.getRowLength), kv)
      }

    hfile.saveAsNewAPIHadoopDataset(conf)

    val loader = new LoadIncrementalHFiles(conf)
    loader.doBulkLoad(new Path(outputPath), htable)
    htable.close()
    FileSystem.get(conf).delete(new Path(outputPath), true)
  }

  // bulkloader
  private def order = new Ordering[SerKeyValue] {
    def compare(a: SerKeyValue, b: SerKeyValue) = {
      new KVComparator().compare(a.buffer, a.offset + 8, Bytes.toInt(a.buffer, a.offset),
        b.buffer, b.offset + 8, Bytes.toInt(b.buffer, b.offset))
    }
  }
}



class RowUpsertExecutor(conn: PhoenixConnection, tableName: String, columnInfoList: Seq[ColumnInfo]) extends Logging {
  val preparedStatement =
    conn.prepareStatement(QueryUtil.constructUpsertStatement(tableName, columnInfoList.asJava))

  val dataTypes = columnInfoList.map(x => PDataType.fromTypeId(x.getSqlType))

  def execute(row: Row): AnyVal = {
    try {
      (0 until row.size).foreach { i =>
        val sqlValue = row.get(i)
        if (sqlValue != null) {
          preparedStatement.setObject(i + 1, sqlValue)
        }
        else {
          preparedStatement.setNull(i + 1, dataTypes(i).getSqlType)
        }
      }
      preparedStatement.execute
    } catch {
      case e: Exception =>
        //        println("Error upserting record", row.toString(), e.getMessage)
        logger.error("Error upserting record", e)
    }

  }
}

class HfilePartitioner(startKeys: Array[Array[Byte]]) extends Partitioner {

  def numPartitions: Int = startKeys.length

  def getPartition(key: Any): Int = {
    val comparator = new RawComparator[ImmutableBytesWritable] {

      def compare(b1: Array[Byte], s1: Int, l1: Int, b2: Array[Byte], s2: Int, l2: Int) = 0

      def compare(a: ImmutableBytesWritable, b: ImmutableBytesWritable) = {
        a.compareTo(b)
      }
    }
    val kv = key.asInstanceOf[SerKeyValue].toKeyValue
    val pos =
      util.Arrays.binarySearch(
        startKeys.map(new ImmutableBytesWritable(_)),
        new ImmutableBytesWritable(kv.getRowArray, kv.getRowOffset, kv.getRowLength), comparator
      ) + 2

    if (Math.abs(pos) >= startKeys.length) startKeys.length - 1
    else Math.abs(pos)
  }

  override def equals(other: Any): Boolean = other match {
    case h: HfilePartitioner => h.numPartitions == numPartitions
    case _ => false
  }

  override def hashCode: Int = numPartitions
}

case class SerKeyValue(buffer: Array[Byte], offset: Int, length: Int) {

  def toKeyValue = new KeyValue(buffer, offset, length)
}

package io.growing.offline.store

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * Created by king on 12/29/16.
  * 定义Hive存储相关的方法
  */
trait HiveStore {

  def store2hive(df: DataFrame, tableName: String,
                 partitions: Seq[(String, Option[String])] = Seq(),
                 isOverWrite: Boolean = true): Unit = {
    val tempTable = s"HiveLoadTask_${tableName.split('.').last}_${Thread.currentThread.getId}"
    df.createOrReplaceTempView(tempTable)

    val insert = if(isOverWrite) {
      s"INSERT OVERWRITE TABLE $tableName"
    } else {
      s"INSERT INTO TABLE $tableName"
    }

    val partition = if(partitions.isEmpty) {
      ""
    } else {
      "PARTITION ( " + partitions.map {
        case (k, None) => k
        case (k, Some(v)) => s"$k = '$v'"
      }.mkString(" , ") + " )"
    }

    val sql =
      s"""
         |$insert $partition
         |SELECT *
         |FROM $tempTable
       """.stripMargin

    df.sparkSession.sql(sql)
  }

  def hiveTableSchema(session: SparkSession, table: String): StructType = {
    session.sql(s"desc $table").collect()
    StructType(Seq())
  }
}

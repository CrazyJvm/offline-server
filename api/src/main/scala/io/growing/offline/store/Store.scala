package io.growing.offline.store

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

/**
  * Created by king on 12/24/16.
  */
trait Store {
  def store(df: DataFrame): Unit = {}

  def store(rdd: RDD[Row]): Unit = {}
}

package io.growing.offline.models

import com.fasterxml.jackson.annotation.JsonIgnoreProperties

/**
  * Created by king on 12/21/16.
  * jobType 用于标记改任务属于hive, hbase, bulkload等哪一类,
  * 调用对应的程序模板
  */
@JsonIgnoreProperties
case class JobInfo(jobGraph: JobGraph, terminalJob: String) {
  override def toString: String = {
    s"jobGraph:\n $jobGraph, terminalJob: $terminalJob"
  }
}

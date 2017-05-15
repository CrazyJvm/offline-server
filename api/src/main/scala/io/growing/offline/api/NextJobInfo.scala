package io.growing.offline.api

/**
  * Created by king on 12/26/16.
  * 这个类与JobInfo类相似,但是为了避免引入server依赖,以及更好地隔离
  * 整个api的接口仅在JobServer, JobActor内引入,避免过多的耦合,所以又在server模块内设计了JobInfo
  */
case class NextJobInfo(job: String, jobType: String,
                       startPos: String, stopPos: String,
                       properties: Map[String, String], priority: Int = 0)

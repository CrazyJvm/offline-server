package io.growing.offline.utils

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneId, ZonedDateTime}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.config.Config
import io.growing.offline.models._

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Try

/**
  * Created by king on 12/21/16.
  */
object ConfigUtils {
  private val formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmm")

  // 单例模式,供全局使用
  val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

  def timeZone(config: Config): ZoneId = ZoneId.of(
    Try(config.getString("app.timeZone"))
      .getOrElse("Asia/Shanghai")
  )

  def parseJobConfigs(config: Config): Set[JobConfig] = {
    config.getConfigList("app.jobs").asScala.map(parseJobConfig).toSet
  }

  def parseJobConfig(c: Config): JobConfig = {
    val name = c.getString("name")
    // jobInfo
    val jobInfo = c.getConfig("jobInfo")

    val terminalJob = Try(jobInfo.getString("terminalJob")).getOrElse(null)
    // 默认为class类型
    val baseJobType = Try(jobInfo.getString("jobType")).getOrElse("class")
    // 默认的共享参数配置为空
    val baseProperties = Try(config2Map(jobInfo.getConfig("properties"))).getOrElse(Map())
    // 默认优先级为3, ddl优先级为0
    val basePriority = Try(jobInfo.getInt("priority")).getOrElse(3)
    // 构建jobGraph
    val jobNodes: mutable.HashMap[String, JobGraphNode] = mutable.HashMap.empty
    val dependencies: mutable.HashMap[String, Seq[String]] = mutable.HashMap.empty
    jobInfo.getConfigList("jobGraph").asScala.foreach { j =>
      val nodeJob = j.getString("job")
      // 从第一个节点默认作为root
      val nodeJobType = Try(j.getString("jobType")).getOrElse(baseJobType)
      // properties配置参数需要合并，具体任务内配置的properties优先级更高，覆盖baseProperties
      val nodeProperties = baseProperties ++ Try(config2Map(j.getConfig("properties"))).getOrElse(Map())
      val nodePriority = Try(j.getInt("priority")).getOrElse(basePriority)
      val node = JobGraphNode(nodeJob, nodeJobType, nodeProperties, nodePriority)
      jobNodes.update(nodeJob, node)
      val followingJobs: Seq[String] = j.getStringList("next").asScala
      followingJobs.foreach { next =>
        // 此处使用getOrElseUpdate，若该node没有在jobGraph中直接定义，则使用默认参数配置
        jobNodes.getOrElseUpdate(next, JobGraphNode(next, nodeJobType, nodeProperties, nodePriority))
      }
      dependencies.update(nodeJob, followingJobs)
    }

    // create job graph
    val graph = JobGraph(jobNodes.toMap)
    graph.initial(dependencies.toMap)

    // process submitInfo
    val submitInfo = c.getConfig("submitInfo")
    val interval = Try(submitInfo.getString("interval")).getOrElse("hourly")
    // if the submitter need to check kafka offsets
    val topicAndGroups: Seq[TopicAndGroup] = Try(
      submitInfo.getConfigList("topicAndGroups").asScala
        .map(c =>
          TopicAndGroup(c.getString("topic"), c.getString("group")))
    ).getOrElse(null)
    // if the submitter need to check it has parent job
    val parent: String = Try(submitInfo.getString("parent")).getOrElse(null)

    JobConfig(name, JobInfo(graph, terminalJob),
      SubmitInfo(interval, topicAndGroups, parent))
  }

  def parseKafkaParams(config: Config): Map[String, String] = {
    val params = config2Map(config.getConfig("app.kafka"))
    if (!params.contains("bootstrap.servers")) {
      throw new Exception("bootstrap.servers is not defined.")
    }
    params
  }

  def parseContextConfigs(config: Config): Map[String, String] = {
    config2Map(config.getConfig("jobContext")) ++
      config2Map(config.getConfig("sparkConfigs"))
  }

  /**
    * 将milliseconds转化为pos, pos的格式为`yyyyMMddHHmm`
    * 采用UTC时区
    * @param time seconds
    * @return
    */
  def parseTime2Pos(time: Long): String = {
    ZonedDateTime.ofInstant(
      Instant.ofEpochSecond(time), ZoneId.of("UTC"))
      .format(formatter)
  }

  def parsePos2Time(pos: String): Long = {
    LocalDateTime.parse(pos, formatter)
      .atZone(ZoneId.of("UTC")).toEpochSecond
  }

  private def config2Map(c: Config): Map[String, String] = {
    val params = mutable.Map[String, String]()
    for (e <- c.entrySet().asScala) {
      params += (e.getKey -> e.getValue.unwrapped().toString)
    }
    params.toMap
  }

}

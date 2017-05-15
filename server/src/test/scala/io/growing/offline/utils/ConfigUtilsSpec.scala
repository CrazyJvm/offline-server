package io.growing.offline.utils

import com.typesafe.config.ConfigFactory
import org.scalatest.{FunSuite, Matchers}
import scala.collection.JavaConverters._

/**
  * Created by king on 12/23/16.
  */
class ConfigUtilsSpec extends FunSuite with Matchers {

  test("parseJobConfigs") {
    val jobConfigs = ConfigUtils.parseJobConfigs(ConfigFactory.load("application-test"))
    println(jobConfigs)
    println(jobConfigs.head.submitInfo.topicAndGroups)
//    val next = jobConfigs.head.jobInfo.properties("next")
//    val nextConfig = ConfigFactory.parseString(s"{ next: $next }")
//    val nextConfigList = nextConfig.getConfigList("next").asScala.foreach(println)
  }

  test("sparkConfigs") {
    val configString =
      """
        |app {
        |  jobServer {
        |    sparkConfigs {
        |      spark.master: "local[2]"
        |      spark.app.name: "local-JobServer"
        |      // 配置到sparkContext.hadoopConfiguration
        |      hadoopConf {
        |        fs.ufile.impl: "org.apache.hadoop.fs.UFileFileSystem"
        |        fs.AbstractFileSystem.ufile.impl: "fs.AbstractFileSystem.ufile.impl"
        |        ufile.properties.file: "conf/ufile.properties"
        |      }
        |    }
        |
        |    jobContext {
        |      // 此处定义所有JobContext内传递的配置参数,例如配置数据库连接
        |      database {
        |        // 数据导出需要的配置AI表
        |        project_settings {
        |          driver: "org.postgresql.Driver"
        |          //    url: "jdbc:postgresql://easydata.cp9cbbjb0tpe.rds.cn-north-1.amazonaws.com.cn:7531/growing"
        |          url: "jdbc:postgresql://localhost:7531/growing"
        |          user: "apps"
        |          password: "v,4)5Y|5G1PzF+fL"
        |        }
        |      }
        |
        |    }
        |  }
        |}
      """.stripMargin
    val config = ConfigUtils.parseContextConfigs(ConfigFactory.parseString(configString))
    println(config)
  }

  test("job as string") {
    val configString =
      """
        |
        |{
        |job: [
        |  {
        |    job: "job1", "jobType": "group", properties { }, priority: 1
        |  }
        |]
        |job1: "job1111"
        |}
        |
      """.stripMargin
    val config = ConfigFactory.parseString(configString)
    println(config.getValue("job").unwrapped().toString)
    println(config.getValue("job1").unwrapped().toString)
  }



}

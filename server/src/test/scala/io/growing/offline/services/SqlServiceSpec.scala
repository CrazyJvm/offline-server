package io.growing.offline.services

import com.typesafe.config.ConfigFactory
import io.growing.offline.models.{JobGraphNode, JobTableRecordStatus}
import kafka.common.TopicAndPartition
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

/**
  * Created by king on 10/5/16.
  */
class SqlServiceSpec extends FunSuite with Matchers with BeforeAndAfterAll {
  val configString: String =
    """
      |app.database {
      |  url = "jdbc:h2:mem:test1"
      |  driver = org.h2.Driver
      |  connectionPool = disabled
      |  keepAliveConnection = true
      |}
    """.stripMargin

  implicit val context = scala.concurrent.ExecutionContext.Implicits.global

  val sqlService = new SqlService(ConfigFactory.parseString(configString))
  sqlService.writeKafkaOffset("t1", "g1",
    "[{\"partition\":2,\"fromOffset\":100,\"untilOffset\":200}]")

  test("fetch offsets from kafkaOffset table successfully") {
    val offsets = Await.result(sqlService.fetchOffsets("t1", "g1"), 100.seconds)
    offsets should equal(Map(TopicAndPartition("t1", 2) -> 199))
  }

  test("could operate job in offline_jobs table") {
    val jobGraphNode = JobGraphNode("testJob", "class", Map(), 0)
    val jobFuture: Future[Int] = sqlService.createJob("201705050800", "201705050900", jobGraphNode, submitName = Some("test"))
    val id = Await.result(jobFuture, 10.seconds)
    var status = Await.result(sqlService.queryJobStatus(id), 10.seconds)
    status should equal(Some(JobTableRecordStatus.CREATED))

    // could update job status in offline_jobs table
    Await.result(sqlService.updateJobStarted(id), 10.seconds)
    status = Await.result(sqlService.queryJobStatus(id), 10.seconds)
    status should equal(Some(JobTableRecordStatus.RUNNING))

    Await.result(sqlService.updateJobFailed(id, "error"), 10.seconds)
    status = Await.result(sqlService.queryJobStatus(id), 10.seconds)
    status should equal(Some(JobTableRecordStatus.FAILED))

    Await.result(sqlService.updateJobFinished(id), 10.seconds)
    val res = Await.result(sqlService.queryFinishedJobsById(idOpt = Some(id)), 10.seconds)
    res.size should equal(1)
  }

  test("could operate job in offline_submit table") {
    val jobFuture: Future[Int] = sqlService.createSubmitJob("test", "201705050800")
    val id = Await.result(jobFuture, 10.seconds)
    var res = Await.result(sqlService.queryLastSubmitJobTimePos("test", finished = true), 10.seconds)
    res should equal(None)

    res = Await.result(sqlService.queryLastSubmitJobTimePos("test"), 10.seconds)
    res should equal(Some("201705050800"))

    Await.result(sqlService.updateSubmitJobFinished(id), 10.seconds)
    res = Await.result(sqlService.queryLastSubmitJobTimePos("test", finished = true), 10.seconds)
    res should equal(Some("201705050800"))
  }

  override def afterAll() {
    sqlService.close()
  }

}

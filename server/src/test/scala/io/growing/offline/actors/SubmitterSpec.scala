package io.growing.offline.actors

import java.time.{LocalDateTime, ZoneId, ZoneOffset}
import java.util.Properties

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestFSMRef, TestKit}
import com.typesafe.config.ConfigFactory
import io.growing.offline.actors.Submitter.{SubmitJobFinished, _}
import io.growing.offline.models._
import io.growing.offline.services.{KafkaService, SqlService}
import io.growing.offline.utils.ConfigUtils
import kafka.producer.{KeyedMessage, Producer}
import kafka.serializer.StringEncoder
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.{TestUtils, TestZKUtils, ZKStringSerializer}
import kafka.zk.EmbeddedZookeeper
import org.I0Itec.zkclient.ZkClient
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration._

/**
  * Created by king on 12/22/16.
  */
class SubmitterSpec(_system: ActorSystem) extends TestKit(_system)
  with ImplicitSender with FunSuiteLike with Matchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("SubmitterSpec"))

  implicit val ec = system.dispatcher

  private val config = ConfigFactory.load("application-test")
  private val jobConfigs = ConfigUtils.parseJobConfigs(config)
  implicit val timeZone: ZoneId = ConfigUtils.timeZone(config)
  private val sqlService = new SqlService(config)

  private val topic: String = "test-topic"
  private val group: String = "test-group"
  private var zkClient: ZkClient = null
  private var zookeeper: EmbeddedZookeeper = null
  private var server: KafkaServer = null
  private var producer: Producer[String, String] = null

  private val kafkaParams = ConfigUtils.parseKafkaParams(config)

  private val kafkaService = new KafkaService(kafkaParams)

  override def beforeAll(): Unit = {
    // 准备Kafka
    val zkUrl = TestZKUtils.zookeeperConnect
    zookeeper = new EmbeddedZookeeper(zkUrl)
    zkClient = new ZkClient(zookeeper.connectString, 6000, 6000, ZKStringSerializer)

    val props = TestUtils.createBrokerConfig(0, 9092, enableControlledShutdown = false)
    props.put("num.partitions", "1")
    server = TestUtils.createServer(new KafkaConfig(props))

    TestUtils.createTopic(zkClient, topic, numPartitions = 1, replicationFactor = 1, servers = Seq(server))

    val properties = new Properties()
    // no need to retry since the send will always fail
    properties.put("message.send.max.retries", "0")
    producer = TestUtils.createProducer[String, String](
      brokerList = "localhost:9092",
      encoder = classOf[StringEncoder].getName,
      keyEncoder = classOf[StringEncoder].getName,
      producerProps = properties)
  }

  override def afterAll(): Unit = {
    producer.close()
    kafkaService.close()

    server.shutdown()

    zkClient.close()
    zookeeper.shutdown()

    sqlService.close()
    TestKit.shutdownActorSystem(system)
  }

  test("submitter uninitialized, construct correctly from jobConfigs") {
    val submitter = TestFSMRef(new Submitter(
      config.getConfig("app.submitter"), jobConfigs, sqlService, kafkaService))

    assert(submitter.stateName == Submitter.Idle)
    assert(submitter.stateData == Submitter.Uninitialized)

    submitter ! SubmitJobConfigList
    expectMsg(jobConfigs)

    val submitName = "test-hourly-jobs1"

    // 检查数据库
    val res = Await.result(sqlService.queryLastSubmitJobTimePos(submitName), 10.seconds)
    res should not be empty

    submitter ! SubmitJobRemove(submitName)

    submitter ! SubmitJobConfigList
    expectMsg(jobConfigs.filter(_.name != submitName))

    // 在offline_submit数据表中插入纪录
    Await.result(sqlService.createSubmitJob(submitName, "201705060200"), 10.seconds)

    // 恢复jobConfigs
    submitter ! SubmitJobAdd(jobConfigs.filter(_.name == submitName).head)

    submitter ! SubmitJobConfigList
    expectMsg(jobConfigs)
  }

  test("submitter Initialized will jump to Active state and schedule jobs") {
    val submitter = TestFSMRef(new Submitter(
      config.getConfig("app.submitter"), jobConfigs, sqlService, kafkaService))

    submitter ! StartSubmitter(testActor)
    expectNoMsg()

    assert(submitter.stateName == Submitter.Active)

    // 往kafka与database内写入对应的message纪录
    val prepareFuture = sqlService.writeKafkaOffset(topic, group,
      """ [{"partition":0,"fromOffset":0,"untilOffset":1}] """).map { _ =>
      // 往kafka发送当前时间戳的数据,使其通过kafka offsets检查
      producer.send(nextMessage(0))
    }
    Await.result(prepareFuture, 10.seconds)

    // submitter 应该处于Active状态，判断test-hourly-jobs1可以提交
    val currentTime = System.currentTimeMillis() / 1000 / 3600 * 3600
    val startPos = ConfigUtils.parseTime2Pos(currentTime - 3600)
    val stopPos = ConfigUtils.parseTime2Pos(currentTime)

    val jobInfo1 = jobConfigs.filter(_.name == "test-hourly-jobs1").head.jobInfo

    submitter ! Flush
    val expectResult1 = ScheduledJobGraph("test-hourly-jobs1",
      startPos, stopPos, jobInfo1.jobGraph, jobInfo1.terminalJob)
    expectMsg(expectResult1)

    submitter ! SubmitJobList
    // 此时test-hourly-jobs1已经提交，但是test-hourly-jobs2还未提交
    val nextStopPos = ConfigUtils.parseTime2Pos(currentTime + 3600)
    val expectResult2 = Map(
      "test-hourly-jobs1" -> (nextStopPos, false),
      "test-hourly-jobs2" -> (stopPos, true)
    )
    expectMsg(expectResult2)

    // 使test-hourly-jobs1结束任务
    submitter ! SubmitJobFinished("test-hourly-jobs1")
    submitter ! SubmitJobList
    val expectResult3 = Map(
      "test-hourly-jobs1" -> (nextStopPos, true),
      "test-hourly-jobs2" -> (stopPos, true)
    )
    expectMsg(expectResult3)

    // 触发scheduleJob，应当返回test-hourly-jobs2
    submitter ! Flush
    val jobInfo2 = jobConfigs.filter(_.name == "test-hourly-jobs2").head.jobInfo
    val expectResult4 = ScheduledJobGraph("test-hourly-jobs2",
      startPos, stopPos, jobInfo2.jobGraph, jobInfo2.terminalJob)
    expectMsg(expectResult4)
  }


  private def nextMessage(hourOffset: Int): KeyedMessage[String, String] = {
    val stm = LocalDateTime.now().plusHours(hourOffset)
      .toEpochSecond(ZoneOffset.UTC) * 1000
    new KeyedMessage[String, String](topic, "k1", s""" { "stm": $stm } """)
  }

}

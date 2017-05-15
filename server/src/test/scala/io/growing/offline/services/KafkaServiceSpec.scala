package io.growing.offline.services

import java.util.Properties

import com.typesafe.config.ConfigFactory
import io.growing.offline.utils.ConfigUtils
import kafka.common.TopicAndPartition
import kafka.producer.{KeyedMessage, Producer}
import kafka.serializer.StringEncoder
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.TestUtils
import kafka.zk.ZooKeeperTestHarness
import org.junit.Test
import org.scalatest.Matchers
import org.scalatest.junit.JUnit3Suite

/**
  * Created by king on 10/5/16.
  */
class KafkaServiceSpec extends JUnit3Suite with ZooKeeperTestHarness with Matchers {
  private val topic: String = "test"

  private var server: KafkaServer = null
  private var producer: Producer[String, String] = null

  override def setUp(): Unit = {
    super.setUp()
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

  override def tearDown(): Unit = {
    kafkaService.close()

    server.shutdown()

    super.tearDown()
  }

  val configString =
    """
      |app {
      |   kafka {
      |    bootstrap.servers: "localhost:9092"
      |    group.id: "jobsubmitter"
      |    fetch.maxRetries: "1"
      |    fetch.maxBytes: "10000"
      |   }
      |}
    """.stripMargin
  val kafkaParams = ConfigUtils.parseKafkaParams(ConfigFactory.parseString(configString))

  val kafkaService = new KafkaService(kafkaParams)

  @Test
  def testKafkaServiceFetchMessages(): Unit = {
    producer.send(new KeyedMessage[String, String](topic, "k1", """ { "stm": 100 } """))

    var msg = kafkaService.fetchMessages(Map(TopicAndPartition(topic, 0) -> 0))
    msg should equal(Map(TopicAndPartition(topic, 0) -> 100))

    msg = kafkaService.fetchMessages(Map(TopicAndPartition(topic, 0) -> 10))
    msg should equal(Map(TopicAndPartition(topic, 0) -> 0))

    println(msg)
  }

}

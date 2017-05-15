package io.growing.offline.services

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

/**
  * Created by king on 2/9/17.
  */
class WeChatClientServiceSpec extends FunSuite with Matchers with BeforeAndAfterAll {
  private val configString =
    """
      |app {
      |  wechat {
      |    corpid: "wx072828b10d7ecaea"
      |    corpsecret: "YN3PoRWHUr6lkkjJzE_4_VLPmGgWH6wFDaQovmicjXba6YuD-6NElZWZX2xaKY1D"
      |  }
      |}
      |
      |akka {
      |  // wechat 发送消息
      |  http.host-connection-pool.client.proxy {
      |    http {
      |      host: "cngtwy"
      |      port: "3128"
      |    }
      |  }
      |}
    """.stripMargin

  private val config = ConfigFactory.parseString(configString)
    .withFallback(ConfigFactory.load())

  implicit val system = ActorSystem.apply("default", config)

  implicit val materializer = ActorMaterializer()

  implicit val executionContext = system.dispatcher

  private var service: WeChatClientService = null
  override def beforeAll(): Unit = {
    service = new WeChatClientService(config)
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  test("send message") {
    service.send("Hanwei is testing job server.")
  }

}

package io.growing.offline.services

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.ActorMaterializer
import com.typesafe.config.Config
import io.growing.offline.models.JacksonSupport
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.util.Try

/**
  * Created by king on 2/8/17.
  */
class WeChatClientService(config: Config)(implicit val system: ActorSystem,
                                          implicit val materializer: ActorMaterializer,
                                          implicit val executor: ExecutionContextExecutor) extends JacksonSupport {
  private val logger = LoggerFactory.getLogger(getClass)

  private val corpid = Try(config.getString("app.wechat.corpid"))
    .getOrElse(throw new Exception("missing corpid for wechat"))
  private val corpsecret = Try(config.getString("app.wechat.corpsecret"))
    .getOrElse(throw new Exception("missing corpsecret fro wechat"))

  private val party: String = Try(config.getString("app.wechat.party")).getOrElse("2")
  private val agent: String = Try(config.getString("app.wechat.agent")).getOrElse("1")

  // 用于发送文件
  private val party1: String = Try(config.getString("app.wechat.party1")).getOrElse("2")
  private val agent1: String = Try(config.getString("app.wechat.agent1")).getOrElse("1")

  private val tokenUrl = "https://qyapi.weixin.qq.com/cgi-bin/gettoken"
  private val baseUrl = "https://qyapi.weixin.qq.com/cgi-bin/message/send"

  private var token: String = fetchToken()

  def send(content: String): Unit = {
    val subContent = content.substring(0, math.min(content.length, 1024))
    val code = sendContentWithToken(subContent, token)
    if (code != 0) {
      token = fetchToken()
      val nextCode = sendContentWithToken(subContent, token)
      if ( nextCode != 0) {
        logger.error(s"send message failed with token: $token, error code: $nextCode")
      }
    }
  }


  private def sendContentWithToken(content: String, currentToken: String): Int = {

    val body = Map("toparty" -> party, "agentid" -> agent, "msgtype" -> "text", "text" -> Map("content" -> content))

    val entity = HttpEntity(ContentTypes.`application/json`, JacksonSupport.defaultObjectMapper.writeValueAsString(body))

    val post = HttpRequest(method = HttpMethods.POST,
      uri = Uri(baseUrl).withRawQueryString(s"access_token=$currentToken"),
      entity = entity)

    val future = Http().singleRequest(post).flatMap {resp =>
      resp.status match {
        case StatusCodes.OK => Unmarshal(resp.entity).to[Map[String, Any]]
          .map(_.apply("errcode").asInstanceOf[Int])
        case _ =>
          Future.failed(new Exception("fetch wechat token error"))
      }
    }

    Await.result(future, 10.seconds)
  }


  private def fetchToken(): String = {
    val get = HttpRequest(method = HttpMethods.GET,
      uri = Uri(tokenUrl).withRawQueryString(s"corpid=$corpid&corpsecret=$corpsecret"))
    val tokenFuture = Http().singleRequest(get).flatMap { resp =>
      resp.status match {
        case StatusCodes.OK => Unmarshal(resp.entity).to[Map[String, Any]]
          .map(_.apply("access_token").toString)
        case _ =>
          Future.failed(new Exception("fetch wechat token error"))
      }
    }
    val token = Await.result(tokenFuture, 10.seconds)
    logger.info("get wechat token: " + token)
    token
  }

}

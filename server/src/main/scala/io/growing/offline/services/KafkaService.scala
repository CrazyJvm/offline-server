package io.growing.offline.services

import java.nio.charset.Charset
import java.util.Properties

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import io.growing.offline.services.KafkaService._
import kafka.api._
import kafka.common.{ErrorMapping, TopicAndPartition}
import kafka.consumer.{ConsumerConfig, SimpleConsumer}
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import scala.util.control.NonFatal

/**
  * Created by king on 10/1/16.
  */
class KafkaService(kafkaParams: Map[String, String]) {
  private val logger = LoggerFactory.getLogger(getClass.getCanonicalName)

  // topic and partition -> consumer
  private val leadersMap: mutable.HashMap[(String, Int), SimpleConsumer] = mutable.HashMap.empty

  // host and port -> consumer
  private val brokersMap: mutable.HashMap[(String, Int), SimpleConsumer] = mutable.HashMap.empty

  private val maxRetries = kafkaParams("fetch.maxRetries").toInt
  private val maxBytes = kafkaParams("fetch.maxBytes").toInt

  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

  // kafka config
  private val config = {
    val props = new Properties()
    kafkaParams.foreach { case (key, value) =>
      // prevent warnings on parameters ConsumerConfig doesn't know about
      if (key != "metadata.broker.list" && key != "bootstrap.servers") {
        props.put(key, value)
      }
    }
    Seq("zookeeper.connect").foreach { s =>
      if (!props.containsKey(s)) {
        props.setProperty(s, "")
      }
    }
    new ConsumerConfig(props)
  }

  private val initBrokers: Seq[(String, Int)] =
    kafkaParams("bootstrap.servers").split(",").map { hp =>
      val hpa = hp.split(":")
      if (hpa.size == 1) {
        throw new Exception(s"Broker not in the correct format of <host>:<port> [$initBrokers]")
      }
      (hpa(0), hpa(1).toInt)
    }.toSeq

  private def connect(host: String, port: Int): SimpleConsumer = {
    brokersMap.getOrElse((host, port), {
      val c = new SimpleConsumer(host, port, config.socketTimeoutMs, config.socketReceiveBufferBytes, config.clientId)
      brokersMap += (host, port) -> c
      c
    })
  }

  private def getPartitionMetadata(topics: Set[String]): Either[Err, Set[TopicMetadata]] = {
    val req = TopicMetadataRequest(
      TopicMetadataRequest.CurrentVersion, 0, config.clientId, topics.toSeq)
    val errs = new Err
    withBrokers(Random.shuffle(initBrokers), errs) { consumer =>
      val resp: TopicMetadataResponse = consumer.send(req)
      val respErrs = resp.topicsMetadata.filter(m => m.errorCode != ErrorMapping.NoError)

      if (respErrs.isEmpty) {
        return Right(resp.topicsMetadata.toSet)
      } else {
        respErrs.foreach { m =>
          val cause = ErrorMapping.exceptionFor(m.errorCode)
          val msg = s"Error getting partition metadata for '${m.topic}'. Does the topic exist?"
          errs.append(new Exception(msg, cause))
        }
      }
    }
    Left(errs)
  }

  private def findLeaders(topicAndPartitions: Set[TopicAndPartition]): Either[Err, Map[TopicAndPartition, (String, Int)]] = {
    val topics = topicAndPartitions.map(_.topic)
    val response = getPartitionMetadata(topics).right
    val answer = response.flatMap { tms: Set[TopicMetadata] =>
      val leaderMap = tms.flatMap { tm: TopicMetadata =>
        tm.partitionsMetadata.flatMap { pm: PartitionMetadata =>
          val tp = TopicAndPartition(tm.topic, pm.partitionId)
          if (topicAndPartitions(tp)) {
            pm.leader.map { l =>
              tp -> (l.host -> l.port)
            }
          } else {
            None
          }
        }
      }.toMap

      if (leaderMap.keys.size == topicAndPartitions.size) {
        Right(leaderMap)
      } else {
        val missing = topicAndPartitions.diff(leaderMap.keySet)
        val err = new Err
        err.append(new Exception(s"Couldn't find leaders for ${missing}"))
        Left(err)
      }
    }
    answer
  }

  private def registerConsumers(offsets: Map[TopicAndPartition, Long]): Unit = {
    val leaders = checkErrors(findLeaders(offsets.keySet))
    leaders.foreach { case (tp, hp) =>
      val key = (tp.topic, tp.partition)
      if (!leadersMap.contains(key) || leadersMap(key).host != hp._1) {
        leadersMap += key -> connect(hp._1, hp._2)
      }
    }
  }

  private def withBrokers(brokers: Iterable[(String, Int)], errs: Err)
                         (fn: SimpleConsumer => Any): Unit = {
    brokers.foreach { hp =>
      var consumer: SimpleConsumer = null
      try {
        consumer = connect(hp._1, hp._2)
        fn(consumer)
      } catch {
        case NonFatal(e) =>
          errs.append(e)
      }
    }
  }

  /**
    * 获取对应offset上消息内的stm
    *
    * @param offsets 传入topicAndPartition与对应的offset值
    * @return topicAndPartition在offset上的消息内的stm
    */
  def fetchMessages(offsets: Map[TopicAndPartition, Long]): Map[TopicAndPartition, Long] = {
    registerConsumers(offsets)
    offsets.par.map { case (tp, offset) =>
      tp -> fetchMessage(tp, offset).getOrElse(0L)
    }.seq
  }

  private def fetchMessage(tp: TopicAndPartition, offset: Long): Option[Long] = {
    var retries = maxRetries
    var fetched = false
    var result: Option[Long] = None
    while (retries > 0 && !fetched) {
      val leader = leadersMap(tp.topic, tp.partition)
      val req = new FetchRequestBuilder()
        .addFetch(tp.topic, tp.partition, offset, config.fetchMessageMaxBytes)
        .build()
      val resp = leader.fetch(req)
      if (resp.hasError) {
        val err = resp.errorCode(tp.topic, tp.partition)
        if ((err == ErrorMapping.LeaderNotAvailableCode ||
          err == ErrorMapping.NotLeaderForPartitionCode) && retries >= 0) {
          logger.warn("Leader Error, Error code: " + err)
          registerConsumers(Map(tp -> offset))
        }
      } else {
        val iter = resp.messageSet(tp.topic, tp.partition).iterator
        if (!iter.hasNext) {
          logger.warn(s"Fetch msg fail: $tp, offset[$offset]")
        } else {
          val messageAndOffset = iter.next()
          val payload = messageAndOffset.message.payload
          val bytes = new Array[Byte](payload.limit())
          payload.get(bytes)
          val stm = mapper.readTree(new String(bytes, Charset.forName("UTF-8"))).path("stm").asLong()
          logger.debug(s"get stm in topic[${tp.topic}], partition[${tp.partition}] at offset[$offset]: $stm")
          result = Some(stm)
          fetched = true
        }
      }

      retries -= 1
    }
    result
  }

  def close(): Unit = {
    leadersMap.values.foreach(_.close())
  }
}

object KafkaService {
  type Err = ArrayBuffer[Throwable]

  /** If the result is right, return it, otherwise throw SparkException */
  def checkErrors[T](result: Either[Err, T]): T = {
    result.fold(
      errs => throw new Exception(errs.mkString("\n")),
      ok => ok
    )
  }

  case class LeaderOffset(host: String, port: Int, offset: Long)

}
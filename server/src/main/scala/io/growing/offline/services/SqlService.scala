package io.growing.offline.services

import java.sql.Timestamp

import com.typesafe.config.Config
import io.growing.offline.models.JobTableRecordStatus.JobTableRecordStatus
import io.growing.offline.models.{JobGraphNode, _}
import io.growing.offline.utils.ConfigUtils
import kafka.common.TopicAndPartition
import org.slf4j.LoggerFactory
import slick.jdbc.PostgresProfile.api._
import slick.jdbc.meta.MTable
import slick.lifted.TableQuery

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor, Future}

/**
  * Created by king on 12/21/16.
  */
class SqlService(config: Config)(implicit context: ExecutionContextExecutor) {
  private val mapper = ConfigUtils.mapper
  private val logger = LoggerFactory.getLogger(getClass)

  private lazy val kafkaOffsetsTable = TableQuery[KafkaOffsetsTable]
  private lazy val jobTable = TableQuery[JobTable]
  private lazy val jobDoneTable = TableQuery[JobDoneTable]
  private lazy val submitTable = TableQuery[SubmitTable]

  private val db = Database.forConfig("app.database", config)

  val tables = List(kafkaOffsetsTable, jobTable, jobDoneTable, submitTable)
  val existing = db.run(MTable.getTables)
  val setupFuture = existing.flatMap( v => {
    val names = v.map(mt => mt.name.name)
    val createIfNotExist = tables.filter( table =>
      !names.contains(table.baseTableRow.tableName)).map(_.schema.create)
    db.run(DBIO.sequence(createIfNotExist))
  })

  Await.result(setupFuture, 10.seconds)

  /************************ kafka_offsets ************************/
  // for test only
  def writeKafkaOffset(topic: String, group: String, offsets: String): Future[Unit] = {
    val actions = DBIO.seq(
      kafkaOffsetsTable += KafkaOffsetsTableRecord(0, topic, group, offsets)
    )
    db.run(actions)
  }

  /**
    * 从数据库获取上一次消费topic&group的kafka offset
    * @param topic kafka的topic
    * @param groupId 消费该topic的groupId
    * @return
    */
  def fetchOffsets(topic: String, groupId: String): Future[Map[TopicAndPartition, Long]] = {
    val query = kafkaOffsetsTable.filter(r => r.topic === topic &&
      r.groupId === groupId && r.is_deleted === false)
      .sortBy(_.created_at.desc).take(1)

    db.run(query.result.headOption).map[Map[TopicAndPartition, Long]]{ recordOpt =>
      if (recordOpt.isDefined) {
        val partitionOffsets: Array[PartitionOffset] =
          mapper.readValue(recordOpt.get.offsets, classOf[Array[PartitionOffset]])
        partitionOffsets.map{ po =>
          TopicAndPartition(topic, po.partition) -> (po.untilOffset - 1)
        }.toMap
      } else {
        Map()
      }
    }
  }

  /************************ offline_jobs ************************/
  /**
    * 在offline_jobs表内创建任务纪录
    */
  def createJob(startPos: String, stopPos: String,
                jobGraphNode: JobGraphNode,
                status: JobTableRecordStatus = JobTableRecordStatus.CREATED,
                submitName: Option[String] = None): Future[Int] = {
    logger.info(s"record job: [${jobGraphNode.job}, ${jobGraphNode.jobType}, " +
      s"$startPos, $stopPos, ${jobGraphNode.properties}")
    val params = mapper.writeValueAsString(jobGraphNode.properties)
    val newJob = JobTableRecord(0, jobGraphNode.job, jobGraphNode.jobType,
      startPos, stopPos, params, jobGraphNode.priority, status,
      submit_name = submitName)
    val insert = jobTable returning jobTable.map(_.id) += newJob
    db.run(insert)
  }

  def createJobs(submitName: String, startPos: String, stopPos: String,
                 jobGraphNodes: Seq[JobGraphNode]): Future[Seq[(String, Int)]] = {
    logger.info(s"record scheduledJobGraph: $submitName, $startPos, $stopPos")
    val newJobs = jobGraphNodes.map { j =>
      JobTableRecord(0, j.job, j.jobType, startPos, stopPos,
        mapper.writeValueAsString(j.properties), j.priority, JobTableRecordStatus.CREATED,
        submit_name = Some(submitName))
    }
    val insert = jobTable returning jobTable.map(_.id) into
      ((item, id) => (item.job, id)) ++= newJobs
    db.run(insert)
  }


  def updateJobStarted(id: Int): Future[Int] = {
    val currentTime = new Timestamp(System.currentTimeMillis())
    val update = jobTable.filter(_.id === id).map(j => (j.status, j.started_at))
      .update(JobTableRecordStatus.RUNNING, Some(currentTime))
    db.run(update)
  }

  def updateJobFailed(id: Int, comment: String): Future[Int] = {
    val currentTime = new Timestamp(System.currentTimeMillis())
    val update = jobTable.filter(_.id === id).map(j => (j.status, j.failed_at, j.comment))
      .update(JobTableRecordStatus.FAILED, Some(currentTime), Some(comment))
    db.run(update)
  }

  // 任务结束时，从offline_jobs移除，写入offline_jobs_done
  def updateJobFinished(id: Int): Future[Int] = {
    val currentTime = new Timestamp(System.currentTimeMillis())

    val update = jobTable.filter(_.id === id).result.headOption.flatMap {
      case Some(r) =>
        jobDoneTable += r.copy(finished_at = Some(currentTime))
      case None =>
        logger.error(s"can not finish job $id, not found in table offline_jobs.")
        jobTable.take(1).result
    }.flatMap { _ =>
      jobTable.filter(_.id === id).delete
    }.transactionally

    db.run(update)
  }


//  def getScheduledJob(id: Int): Future[Option[ScheduledJob]] = {
//    db.run(jobTable.filter(_.id === id).result.headOption)
//      .map(_.map { r =>
//        val properties = mapper.readValue(r.params, classOf[Map[String, String]])
//        ScheduledJob(r.id, JobInfo(r.job, r.jobType, properties, r.priority))
//      })
//  }

  /**
    * 根据job id查询任务运行状态,若job已经完成(不存在),则返回None
    *
    * @param id 创建任务时获取的job id
    * @return 返回任务状态: CREATED, FAILED, RUNNING,
    */
  def queryJobStatus(id: Int): Future[Option[JobTableRecordStatus]] = {
    val query = jobTable.filter(_.id === id).map(_.status)
    db.run(query.result.headOption)
  }

//  def lastSubmitTime(submitName: String): Future[Option[Option[Timestamp]]] = {
//    val query = jobTable.filter(j => j.submit_name.isDefined
//      && j.submit_name === submitName
//      && j.submit_time.isDefined && j.status === 2)
//      .map(_.submit_time)
//      .sortBy(_.desc).take(1).result.headOption
//    db.run(query)
//  }
//
//  def cleanLastUnfinishedSubmitJob(submitName: String): Future[Int]= {
//    val delete = jobTable.filter(j => j.submit_name.isDefined &&
//      j.submit_name === submitName && j.status =!= 2).delete
//    db.run(delete)
//  }

  /**
    * 从数据库恢复所有未结束的(包含Started, Failed, Created)的Job
    * @return
    */
  def queryAllJobs(): Future[Seq[JobTableRecord]] = {
    db.run(jobTable.result)
  }

  def queryJobById(id: Int): Future[Option[JobTableRecord]] = {
    db.run(jobTable.filter(_.id === id).result.headOption)
  }

  /************************ offline_jobs_done ************************/

  // 该表用于历史纪录，可用于查询job信息
  def queryFinishedJobsById(idOpt: Option[Int] = None): Future[Seq[JobTableRecord]] = {
    val query = idOpt match {
      case Some(id) =>
        jobDoneTable.filter(_.id === id)
      case None =>
        jobDoneTable.sortBy(_.created_at.desc).take(100)
    }
    db.run(query.result)
  }

  def queryFinishedJobsBySubmitName(submit_name: String): Future[Seq[JobTableRecord]] = {
    db.run(
      jobDoneTable.filter(_.submit_name === submit_name)
        .sortBy(_.created_at.desc).take(100).result
    )
  }

  /************************ offline_submit ************************/
  def createSubmitJob(submitName: String, submitTimePos: String): Future[Int] = {
    val insert = submitTable returning submitTable.map(_.id) += SubmitTableRecord(0, submitName, submitTimePos)
    db.run(insert)
  }

  def updateSubmitJobFinished(id: Int): Future[Int] = {
    val currentTime = new Timestamp(System.currentTimeMillis())
    val update = submitTable.filter(_.id === id).map(j => j.finished_at)
      .update(Some(currentTime))
    db.run(update)
  }

  def queryLastSubmitJobTimePos(submitName: String, finished: Boolean = false): Future[Option[(String, Int)]] = {
    val query = submitTable.filter(j =>
      j.submit_name === submitName &&
      j.is_deleted === false &&
        (if (finished) j.finished_at.isDefined else j.finished_at.isEmpty)
    ).sortBy(_.created_at.desc).map(j => (j.submit_time_pos, j.id))

    db.run(query.result.headOption)
  }

  def close(): Unit = {
    db.close()
  }

}

case class PartitionOffset(partition: Int, fromOffset: Long, untilOffset: Long)
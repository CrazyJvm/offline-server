package io.growing.offline.models

import java.sql.Timestamp

import io.growing.offline.models.JobTableRecordStatus.JobTableRecordStatus
import slick.jdbc.PostgresProfile.api._

/**
  * Created by king on 12/21/16.
  */
class JobTable(tag: Tag) extends Table[JobTableRecord](tag, "offline_jobs") {
  def id = column[Int]("id", O.PrimaryKey, O.AutoInc)
  def job = column[String]("job")
  def job_type = column[String]("type")
  def start_pos = column[String]("start_pos")
  def stop_pos = column[String]("stop_pos")
  def properties = column[String]("properties")
  def priority = column[Int]("priority", O.Default(0))
  def status = column[JobTableRecordStatus]("status", O.Default(JobTableRecordStatus.CREATED))
  def created_at = column[Timestamp]("created_at", O.Default(new Timestamp(System.currentTimeMillis())))
  def started_at = column[Option[Timestamp]]("started_at")
  def failed_at = column[Option[Timestamp]]("failed_at")
  def comment = column[Option[String]]("comment")

  // 该字段用于纪录该job由哪一个submit job提交，恢复系统时按照该字段寻找对应的job graph
  // 若无该字段，则表示该job不是submit的方式提交的，而是ad-hoc的任务
  def submit_name = column[Option[String]]("submit_name")

  // job_id = job + md5(properties).substring(0, 8)
//  def job_id = column[String]("job_id")

  def * = (id, job, job_type, start_pos, stop_pos, properties, priority, status,
    created_at, started_at, failed_at, comment, submit_name) <>
    (JobTableRecord.tupled, JobTableRecord.unapply)
}

/**
  *
  * @param id           任务
  * @param job          任务对应的类名称,或者配置文件名称
  * @param properties   任务执行参数
  * @param priority     优先级,0~9逐渐递减, -1优先级代表直接运行
  * @param status       job的状态, 0代表创建,1代表正在运行,2代表运行结束,3代表运行失败
  * @param created_at   任务创建时间
  * @param started_at   任务开始时间
  * @param finished_at  任务结束时间,任务失败或者结束时写入
  * @param comment      若是运行失败,则写入exception信息
  */
case class JobTableRecord(id: Int, job: String, jobType: String,
                          startPos: String, stopPos: String,
                          properties: String, priority: Int = 0,
                          status: JobTableRecordStatus = JobTableRecordStatus.CREATED,
                          created_at: Timestamp = new Timestamp(System.currentTimeMillis()),
                          started_at: Option[Timestamp] = None,
                          finished_at: Option[Timestamp] = None,
                          comment: Option[String] = None,
                          submit_name: Option[String] = None)

object JobTableRecordStatus extends Enumeration {
  type JobTableRecordStatus = Value
  val CREATED   = Value("created")
  val RUNNING   = Value("running")
  val SUCCEEDED = Value("succeeded")
  val FAILED    = Value("failed")
}


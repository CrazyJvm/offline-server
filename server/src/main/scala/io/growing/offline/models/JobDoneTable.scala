package io.growing.offline.models

import java.sql.Timestamp

import io.growing.offline.models.JobTableRecordStatus.JobTableRecordStatus
import slick.jdbc.PostgresProfile.api._

/**
  * Created by king on 4/13/17.
  */
class JobDoneTable(tag: Tag) extends Table[JobTableRecord](tag, "offline_jobs_done") {
  def id = column[Int]("id", O.PrimaryKey, O.AutoInc)
  def job = column[String]("job")
  def job_type = column[String]("type")
  def start_pos = column[String]("start_pos")
  def stop_pos = column[String]("stop_pos")
  def properties = column[String]("properties")
  def priority = column[Int]("priority", O.Default(0))
  def status = column[JobTableRecordStatus]("status", O.Default(JobTableRecordStatus.SUCCEEDED))
  def created_at = column[Timestamp]("created_at", O.Default(new Timestamp(System.currentTimeMillis())))
  def started_at = column[Option[Timestamp]]("started_at")
  def finished_at = column[Option[Timestamp]]("finished_at")
  def comment = column[Option[String]]("comment")

  // 该字段用于标记上一次提交的任务时间点,同时用于区分是否由submitter提交
  def submit_name = column[Option[String]]("submit_name")

  // job_id = job + md5(properties).substring(0, 8)
//  def job_id = column[String]("job_id")

  def * = (id, job, job_type, start_pos, stop_pos, properties, priority, status,
    created_at, started_at, finished_at, comment, submit_name) <>
    (JobTableRecord.tupled, JobTableRecord.unapply)
}

package io.growing.offline.models

import java.sql.Timestamp

import slick.jdbc.PostgresProfile.api._
/**
  * Created by king on 4/14/17.
  */
class SubmitTable(tag: Tag) extends Table[SubmitTableRecord](tag, "offline_submit") {
  def id = column[Int]("id", O.PrimaryKey, O.AutoInc)
  def submit_name = column[String]("submit_name")
  // 对应提交的时间点
  def submit_time_pos = column[String]("submit_time_pos")

  def created_at = column[Timestamp]("created_at", O.Default(new Timestamp(System.currentTimeMillis())))
  // 这一次submit结束的时间点
  def finished_at = column[Option[Timestamp]]("finished_at")

  // 考虑重跑的情况，设置id_deleted参数，默认false
  def is_deleted = column[Boolean]("is_deleted", O.Default(false))

  def * = (id, submit_name, submit_time_pos, created_at, finished_at, is_deleted) <>
    (SubmitTableRecord.tupled, SubmitTableRecord.unapply)
}


case class SubmitTableRecord(id: Int, submitName: String,
                             submitTimePos: String,
                             createdTime: Timestamp = new Timestamp(System.currentTimeMillis()),
                             finishedTime: Option[Timestamp] = None,
                             isDeleted: Boolean = false)
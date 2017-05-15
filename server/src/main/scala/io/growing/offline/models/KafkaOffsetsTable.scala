package io.growing.offline.models

import java.sql.Timestamp
import slick.jdbc.PostgresProfile.api._

/**
  * Created by king on 10/1/16.
  */
case class KafkaOffsetsTableRecord(id: Int,
                       topic: String,
                       groupId: String,
                       offsets: String,
                       created_at: Timestamp = new Timestamp(System.currentTimeMillis()),
                       updated_at: Timestamp = new Timestamp(System.currentTimeMillis()),
                       is_deleted: Boolean = false)

class KafkaOffsetsTable(tag: Tag) extends Table[KafkaOffsetsTableRecord](tag, "kafka_offsets"){
  def id = column[Int]("id", O.PrimaryKey, O.AutoInc)
  def topic = column[String]("topic")
  def groupId = column[String]("group_id")
  def offsets = column[String]("offsets")
  def created_at = column[Timestamp]("created_at", O.Default(new Timestamp(System.currentTimeMillis())))
  def updated_at = column[Timestamp]("updated_at", O.Default(new Timestamp(System.currentTimeMillis())))
  def is_deleted = column[Boolean]("is_deleted", O.Default(false))

  def * = (id, topic, groupId, offsets, created_at, updated_at, is_deleted) <>
    (KafkaOffsetsTableRecord.tupled, KafkaOffsetsTableRecord.unapply)
}

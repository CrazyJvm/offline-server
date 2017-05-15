package io.growing.offline.models

/**
  * Created by king on 12/21/16.
  * interval === hourly(default), daily, monthly, half-hourly...
  * submitInfo有两种方式提交任务，一种为topicAndGroups
  */
case class SubmitInfo(interval: String,
                      topicAndGroups: Seq[TopicAndGroup],
                      parent: String)

case class TopicAndGroup(topic: String, group: String) {
  override def toString: String = {
    s"topic = $topic and group = $group"
  }
}
package io.growing.offline.models

import io.growing.offline.actors.JobScheduler.ScheduledJob

import scala.collection.mutable.ArrayBuffer

/**
  * Created by king on 4/12/17.
  * 用于某个时间点的任务依赖图
  */
case class ScheduledJobGraph(name: String, startPos: String, stopPos: String,
                             jobGraph: JobGraph, terminalJob: String) {

  // 用于标记某个任务是否已经运行完成
  val marked: Array[Boolean] = {
    val _marked = new Array[Boolean](jobGraph.jobNodes.size)
    _marked.indices.foreach {
      i => _marked.update(i, false)
    }
    _marked
  }

  val scheduled: Array[Boolean] = marked.clone()

  // 我还是比较喜欢用null，不要拘谨于Scala的FP特性，同时充分利用OO的特性
  var registeredJobs: Map[String, Int] = null

  // 当所有的job都已经写入数据库之后，需要将数据库内的id信息注册到ScheduledJobGraph中
  def initial(registeredJobs: Map[String, Int]): Unit = {
    // 将graph写入数据库持久化时，
    (jobGraph.jobNodes.keySet -- registeredJobs.keySet).map(jobGraph.jobNodesIndex).foreach { i =>
      marked(i) = true
    }

    this.registeredJobs = registeredJobs
  }

  // get the following jobs which could be scheduled to run
  def scheduleJobs(): Seq[ScheduledJob] = {
    val jobs: ArrayBuffer[ScheduledJob] = ArrayBuffer.empty
    jobGraph.matrix.indices.foreach { i =>
      // 找到尚未运行过的节点, and 尚未提交过的节点, and (没有依赖任务，或者依赖任务都已经为true)
      if (!marked(i) && !scheduled(i) && (jobGraph.matrix(i).isEmpty || jobGraph.matrix(i).forall(marked))) {
        val jobGraphNode = jobGraph.indexToJobNodes(i)
        jobs.append(ScheduledJob(registeredJobs(jobGraphNode.job),
          ScheduledJobGraphNode(startPos, stopPos, jobGraphNode))
        )
        scheduled(i) = true
      }
    }
    jobs
  }

  // update mark
  def updateMark(jobNodeId: String): Unit = synchronized {
    marked(jobGraph.jobNodesIndex(jobNodeId)) = true
  }

  def jobGraphFinished(): Boolean = {
    marked.indices.forall(marked)
  }

  def terminalJobFinished(): Boolean = {
    if (terminalJob == null) {
      jobGraphFinished()
    } else {
      marked(jobGraph.jobNodesIndex(terminalJob))
    }
  }

  def getScheduledJobGraphNode(jobNodeId: String): ScheduledJobGraphNode = {
    ScheduledJobGraphNode(startPos, stopPos, jobGraph.jobNodes(jobNodeId))
  }

}

case class ScheduledJobGraphNode(startPos: String, stopPos: String, jobGraphNode: JobGraphNode)
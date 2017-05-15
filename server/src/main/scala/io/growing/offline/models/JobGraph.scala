package io.growing.offline.models

import org.apache.commons.codec.digest.DigestUtils

import scala.collection.mutable.ArrayBuffer

/**
  * Created by king on 4/11/17.
  * jobNodes:  job -> JobGraphNode
  * 所以此处job名称在jobGraph内必须唯一
  */
case class JobGraph(jobNodes: Map[String, JobGraphNode]) {
  val matrix: Array[ArrayBuffer[Int]] = new Array[ArrayBuffer[Int]](jobNodes.size)

  val jobNodesIndex: Map[String, Int] = jobNodes.keys.zipWithIndex.toMap
  val indexToJobNodes: Map[Int, JobGraphNode] = jobNodesIndex.map(k => k._2 -> jobNodes(k._1))

  def initial(dependencies: Map[String, Seq[String]]): Unit = {
    for (i <- 0 until jobNodes.size) {
      matrix(i) = ArrayBuffer.empty
    }
    dependencies.foreach { d1 =>
      val from = jobNodesIndex(d1._1)
      d1._2.foreach { d2 =>
        matrix(jobNodesIndex(d2)).append(from)
      }
    }
  }

  override def toString: String = {
    val sb = new StringBuilder()
    sb.append("matrix: \n")
    matrix.indices.foreach { i =>
      sb.append(s"${indexToJobNodes(i).job}[$i]:\t")
      sb.append(matrix(i).map(indexToJobNodes).map(_.job).mkString("\t"))
      sb.append("\n")
    }
    sb.toString()
  }

}

// 定义在job内部的属性，能够覆盖在JobGraph中共有的部分
case class JobGraphNode(job: String, jobType: String,
                        properties: Map[String, String],
                        priority: Int) {
//  val jobNodeId: String = s"$job-${DigestUtils.md5Hex(properties.mkString).substring(0, 8)}"
}
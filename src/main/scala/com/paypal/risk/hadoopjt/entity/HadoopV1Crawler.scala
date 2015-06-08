/*
 * Copyright 2015 Danick Zhu(guazhu@paypal.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.paypal.risk.hadoopjt.entity

import scala.xml.XML
import scala.xml.Elem
import scala.xml.Node
import scala.collection.mutable.ListBuffer

import com.paypal.risk.hadoopjt.JobTracker
import com.paypal.risk.hadoopjt.util._
import EntityTraits._

/**
 * @author Danick
 */
object HadoopV1Crawler extends JobTracker {

  var urlLoader: String => String = NetUtils.getTidyHtmlFromURL

  case class MapReduceV1Job(val jobUrl: String,
    val jobId: String,
    val priority: String,
    val user: String,
    val name: String,
    val pool: String) extends ListableObject {
    override def toList() = List(this.jobId, this.priority, this.user, this.pool, this.name, finishedTotal(this.mapCompletePercent, this.mapComplete, this.mapTotal), finishedTotal(this.reduceComletePercent, this.reduceComlete, this.reduceTotal), this.runningTime)
    def finishedTotal(percent: String, finished: String, total: String) = percent + " - " + finished + "/" + total
    var mapCompletePercent: String = null
    var mapTotal: String = null
    var mapComplete: String = null
    var mapKilled: String = null
    var reduceComletePercent: String = null
    var reduceTotal: String = null
    var reduceComlete: String = null
    var reduceKilled: String = null
    var runningTime: String = null
    var jobInfoData: Seq[Seq[String]] = null
  }

  object MapReduceV1Job extends SchemaObject {
    override def getSchema() = List("JobId", "Priority", "User", "Queue", "Name", "MapComplete", "ReduceComplete", "RunningTime")
    var jobInfoSchema: Seq[String] = null
  }

  case class HadoopQueue(val name: String,
    val runningJobs: String,
    val mapMinShare: String,
    val mapMaxShare: String,
    val mapRunning: String,
    val mapFairShare: String,
    val reduceMinShare: String,
    val reduceMaxShare: String,
    val reduceRunning: String,
    val reduceFairShare: String,
    val schedulingMode: String) extends ListableObject {
    override def toList() = List(this.name, this.runningJobs, this.mapMinShare, this.mapMaxShare, this.mapRunning, this.mapFairShare, this.reduceMinShare,
      this.reduceMaxShare, this.reduceRunning, this.reduceFairShare, this.schedulingMode)
  }

  object HadoopQueue extends SchemaObject {
    override def getSchema() = List("Name", "RunningJobs", "MapMinShare", "MapMaxShare", "MapRunning", "MapFairShare", "ReduceMinShare",
      "ReduceMaxShare", "ReduceRunning", "ReduceFairShare", "SchedulingMode")
  }

  case class HadoopV1ClusterInfo(val runMapTask: String,
    val runReduceTask: String,
    val nodes: String,
    val occupiedMapSlots: String,
    val occupiedReduceSlots: String,
    val reservedMapSlots: String,
    val reservedReduceSlots: String,
    val mapTaskCapacity: String,
    val reduceTaskCapacity: String,
    val averageTasksPerNode: String,
    val blacklistedNodes: String,
    val excludedNodes: String) extends ListableObject {
    override def toList() = List(this.runMapTask, this.runReduceTask, this.occupiedMapSlots, this.occupiedReduceSlots, this.reservedMapSlots,
      this.reservedReduceSlots, this.mapTaskCapacity, this.reduceTaskCapacity, this.averageTasksPerNode, this.blacklistedNodes, this.excludedNodes)
  }
  object HadoopV1ClusterInfo extends SchemaObject {
    override def getSchema() = List("Running Map Tasks", "Running Reduce Tasks",
      "Nodes", "Occupied Map Slots", "Occupied Reduce Slots", "Reserved Map Slots", "Reserved Reduce Slots",
      "Map Task Capacity", "Reduce Task Capacity", "Avg. Tasks/Node", "Blacklisted Nodes", "Excluded Nodes")
  }

  override def getClusterInfo() = {
    val document = loadHtmlDocument(Constants.JT_HTTP_BASE_URL + "/jobtracker.jsp")
    val clusterTable = (document \ "body" \ "table")(0)
    (clusterTable \\ "tr").filter(line => (line \ "td").length > 0).map { tr =>
      val clusterInfoColumns = tr \ "td"
      val runMapTask: String = clusterInfoColumns(0).text
      val runReduceTask: String = clusterInfoColumns(1).text
      val nodes: String = clusterInfoColumns(2).text
      val occupiedMapSlots: String = getText(clusterInfoColumns(3))
      val occupiedReduceSlots: String = clusterInfoColumns(4).text
      val reservedMapSlots: String = clusterInfoColumns(5).text
      val reservedReduceSlots: String = clusterInfoColumns(6).text
      val mapTaskCapacity: String = clusterInfoColumns(7).text
      val reduceTaskCapacity: String = clusterInfoColumns(8).text
      val averageTasksPerNode: String = clusterInfoColumns(9).text
      val blacklistedNodes: String = getText(clusterInfoColumns(10))
      val excludedNodes: String = getText(clusterInfoColumns(11))
      new HadoopV1ClusterInfo(runMapTask, runReduceTask, nodes, occupiedMapSlots, occupiedReduceSlots, reservedMapSlots, reservedReduceSlots,
        mapTaskCapacity, reduceTaskCapacity, averageTasksPerNode, blacklistedNodes, excludedNodes)
    }
  }

  override def getQueuesInfo() = {
    val document = loadHtmlDocument(Constants.JT_HTTP_BASE_URL + "/scheduler")
    val queueTable = (document \ "body" \ "table")(0)

    (queueTable \ "tbody" \ "tr").filter(line => (line \ "td").length > 0).map { tr =>
      val queueInfoColumns = tr \ "td"
      val name = queueInfoColumns(0).text
      val runningJobs = queueInfoColumns(1).text
      val mapMinShare = queueInfoColumns(2).text
      val mapMaxShare = queueInfoColumns(3).text
      val mapRunning = queueInfoColumns(4).text
      val mapFairShare = queueInfoColumns(5).text
      val reduceMinShare = queueInfoColumns(6).text
      val reduceMaxShare = queueInfoColumns(7).text
      val reduceRunning = queueInfoColumns(8).text
      val reduceFairShare = queueInfoColumns(9).text
      val schedulingMode = queueInfoColumns(10).text
      new HadoopQueue(name, runningJobs, mapMinShare, mapMaxShare, mapRunning, mapFairShare, reduceMinShare, reduceMaxShare, reduceRunning, reduceFairShare, schedulingMode)
    }
  }

  def parseAllJobs() = {
    val document = loadHtmlDocument(Constants.JT_HTTP_BASE_URL + "/scheduler")
    val jobTable = (document \ "body" \ "table")(1)
    (jobTable \ "tbody" \ "tr").filter(line => (line \ "td").length > 0).map { tr =>
      val jobInfoRow = tr \ "td"
      val jobUrl = (jobInfoRow(1) \ "a" \ "@href").text
      val jobId = (jobInfoRow(1) \ "a").text
      val user = jobInfoRow(2).text
      val name = jobInfoRow(3).text
      val pool = (jobInfoRow(4) \\ "option").filter(option => option.attributes.asAttrMap.contains("selected"))(0).text
      val priority = (jobInfoRow(5) \\ "option").filter(option => option.attributes.asAttrMap.contains("selected"))(0).text
      new MapReduceV1Job(jobUrl, jobId, priority, user, name, pool)
    }
  }

  /*
   * Parse the following information from job url page
   * ╔══════╤══════════╤═════════╤═══════╤═══════╤════════╤══════╤══════════════════════════╗
   * ║Kind  │% Complete│Num Tasks│Pending│Running│Complete│Killed│Failed/KilledTask Attempts║
   * ╟──────┼──────────┼─────────┼───────┼───────┼────────┼──────┼──────────────────────────╢
   * ║map   │34.22%    │371      │0      │371    │0       │0     │4                         ║
   * ╟──────┼──────────┼─────────┼───────┼───────┼────────┼──────┼──────────────────────────╢
   * ║reduce│0.00%     │0        │0      │0      │0       │0     │0 / 0                     ║
   * ╚══════╧══════════╧═════════╧═══════╧═══════╧════════╧══════╧══════════════════════════╝
   */
  def fullfillMapReduceJob(jobs: Seq[MapReduceV1Job]) = {
    jobs.map { job =>
      val jobInfoDocument = loadHtmlDocument(Constants.JT_HTTP_BASE_URL + "/" + job.jobUrl)
      val jobInfoDetails = getMapReduceTaskDetail(jobInfoDocument)
      val jobInfoSchema = jobInfoDetails.head
      val jobInfoData = jobInfoDetails.tail

      job.mapComplete = jobInfoData(0)(5)
      job.mapKilled = jobInfoData(0)(6)
      job.mapTotal = jobInfoData(0)(2)
      job.mapCompletePercent = jobInfoData(0)(1)
      job.reduceComlete = jobInfoData(1)(5)
      job.reduceKilled = jobInfoData(1)(6)
      job.reduceTotal = jobInfoData(1)(2)
      job.reduceComletePercent = jobInfoData(1)(1)
      job.runningTime = parseJobRunningTime(jobInfoDocument)
      MapReduceV1Job.jobInfoSchema = jobInfoSchema
      job.jobInfoData = jobInfoData
      job
    }
  }

  def loadHtmlDocument(url: String) = {
    XML.loadString(urlLoader(url))
  }

  def parseJobRunningTime(document: Elem) = {
    (document \ "body")(0).child(44).text.trim()
  }

  def getMapReduceTaskDetail(document: Elem) = {
    val targetTable = (document \ "body" \ "table")(0)
    val result = new ListBuffer[Seq[String]]
    result += ((targetTable \ "tbody" \ "tr")(0) \ "th").map { th =>
      th.text
    }
    result ++= (targetTable \ "tbody" \ "tr").tail map { tr =>
      val subNodeList = new ListBuffer[Node]
      subNodeList += (tr \ "th")(0)
      subNodeList ++= (tr \ "td")
      subNodeList map { e =>
        if ((e \ "a").length > 0) {
          (e \ "a")(0).text.trim()
        } else {
          e.text.trim
        }
      }
    }
    result
  }

  /**
   * get node's text, or get first sub's text if there's any<br/>
   * used for some nodes have &lt;a&gt; tag, and sometimes not
   * @param node
   * @param sub
   * @return
   */
  def getText(node: Node, sub: String = "a") = {
    if ((node \ sub).length > 0) {
      (node \ sub)(0).text
    } else {
      node.text
    }
  }

  def getJobTaskIds(url: String) = {
    val document = loadHtmlDocument(url)
    val targetTable = (document \ "body" \\ "table")(0)
    if (targetTable != null) {
      val tabletr = (targetTable \ "tbody" \ "tr")
      tabletr map { tr =>
        ((tr \ "td")(0) \ "a")(0).text.trim()
      }
    } else {
      null
    }
  }

  def getTaskAttemptIds(taskurl: String, status: String) = {
    val document = loadHtmlDocument(taskurl)
    val targetTable = (document \ "body" \ "center" \ "table")(0)
    if (targetTable != null) {
      val tabletr = (targetTable \ "tbody" \ "tr")
      tabletr.filter { tr => (tr \ "td")(2).text.equalsIgnoreCase(status) } map { tr =>
        ((tr \ "td")(0).text.trim(), ((tr \ "td")(1) \ "a").text.split("/").last)
      }
    } else {
      null
    }
  }

}

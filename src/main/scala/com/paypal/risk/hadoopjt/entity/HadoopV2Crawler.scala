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

import org.json.{JSONArray, JSONObject}
import org.slf4j.LoggerFactory
import scala.language.implicitConversions
import scala.collection.mutable.ListBuffer

import com.paypal.risk.hadoopjt.JobTracker
import com.paypal.risk.hadoopjt.util._
import EntityTraits._

/**
 * @author Danick
 */
object HadoopV2Crawler extends JobTracker {

  val logger = LoggerFactory.getLogger(getClass())

  implicit def IntToString(data: Int): String = data.toString()

  implicit def LongToString(data: Long): String = data.toString()

  implicit def DoubleToString(data: Double): String = data.toString()

  var urlLoader: String => String = NetUtils.httpGet

  case class ClusterMetrics(val appsSubmitted: String,
                            val appsCompleted: String,
                            val appsPending: String,
                            val appsRunning: String,
                            val appsFailed: String,
                            val appsKilled: String,
                            val reservedMB: String,
                            val availableMB: String,
                            val allocatedMB: String,
                            val containersAllocated: String,
                            val containersReserved: String,
                            val containersPending: String,
                            val totalMB: String,
                            val totalNodes: String,
                            val lostNodes: String,
                            val unhealthyNodes: String,
                            val decommissionedNodes: String,
                            val rebootedNodes: String,
                            val activeNodes: String) extends ListableObject {
    override def toList = List(this.appsSubmitted, this.appsCompleted,
      this.appsPending, this.appsRunning, this.appsFailed,
      this.appsKilled, this.reservedMB, this.availableMB,
      this.allocatedMB, this.containersAllocated,
      this.containersReserved, this.containersPending,
      this.totalMB, this.totalNodes, this.lostNodes,
      this.unhealthyNodes, this.decommissionedNodes,
      this.rebootedNodes, this.activeNodes)
  }

  object ClusterMetrics extends SchemaObject {
    override def getSchema = List("appsSubmitted", "appsCompleted",
      "appsPending", "appsRunning", "appsFailed", "appsKilled", "reservedMB",
      "availableMB", "allocatedMB", "containersAllocated", "containersReserved",
      "containersPending", "totalMB", "totalNodes", "lostNodes", "unhealthyNodes",
      "decommissionedNodes", "rebootedNodes", "activeNodes")
  }

  override def getClusterInfo() = {
    val clusterMetricsStr = loadRestfulAPI(Constants.JT_HTTP_BASE_URL + "/ws/v1/cluster/metrics")
    val clusterMetrics = new JSONObject(clusterMetricsStr).getJSONObject("clusterMetrics");
    List(new ClusterMetrics(clusterMetrics.getInt("appsSubmitted"),
      clusterMetrics.getInt("appsCompleted"), clusterMetrics.getInt("appsPending"),
      clusterMetrics.getInt("appsRunning"), clusterMetrics.getInt("appsFailed"),
      clusterMetrics.getInt("appsKilled"), clusterMetrics.getLong("reservedMB"),
      clusterMetrics.getLong("availableMB"), clusterMetrics.getLong("allocatedMB"),
      clusterMetrics.getLong("containersAllocated"), clusterMetrics.getLong("containersReserved"),
      clusterMetrics.getLong("containersPending"), clusterMetrics.getInt("totalMB"),
      clusterMetrics.getInt("totalNodes"), clusterMetrics.getInt("lostNodes"),
      clusterMetrics.getInt("unhealthyNodes"), clusterMetrics.getInt("decommissionedNodes"),
      clusterMetrics.getInt("rebootedNodes"), clusterMetrics.getInt("activeNodes")))
  }

  //TODO enhance the data
  case class SchedulerQueue(val queueName: String,
                            val capacity: String,
                            val maxCapacity: String,
                            val usedCapacity: String,
                            val numApplications: String,
                            val maxApplicationsPerUser: String,
                            val numContainers: String) extends ListableObject {
    override def toList: List[String] = List(this.queueName, this.numApplications, this.capacity, this.maxCapacity, this.usedCapacity, this.maxApplicationsPerUser, this.numContainers)
  }

  object SchedulerQueue extends SchemaObject {
    override def getSchema: List[String] = List("Name", "NumApps", "Capacity",
      "MaxCapacity", "CurrentCapacity", "MaxAppsPerUser", "#RunningContainers")
  }

  override def getQueuesInfo() = {
    val schedulerJson = loadRestfulAPI(Constants.JT_HTTP_BASE_URL + "/ws/v1/cluster/scheduler")
    val rootQueues: JSONArray = new JSONObject(schedulerJson).
      getJSONObject("scheduler").
      getJSONObject("schedulerInfo")
      .getJSONObject("queues").getJSONArray("queue");

    parseQueueArrayInfo(rootQueues, "")
  }

  def parseQueueArrayInfo(queue: JSONArray, prefix: String): ListBuffer[SchedulerQueue] = {
    val listBuffer = ListBuffer[SchedulerQueue]()
    for (i <- 0 until queue.length()) {
      listBuffer ++= parseQueueInfo(queue.getJSONObject(i), prefix)
    }
    listBuffer
  }

  def parseQueueInfo(queue: JSONObject, prefix: String) = {
    val listBuffer = ListBuffer[SchedulerQueue]()
    var maxApplicationsPerUser: String = ""
    var numContainers: String = ""
    if (!queue.has("queues")) {
      maxApplicationsPerUser = queue.getInt("maxApplicationsPerUser")
      numContainers = queue.getInt("numContainers")
    }
    val queueInfo = new SchedulerQueue(prefix + queue.getString("queueName"), queue.getDouble("capacity"),
      queue.getDouble("maxCapacity"), queue.getDouble("usedCapacity"), queue.getInt("numApplications"), maxApplicationsPerUser, numContainers)
    listBuffer.append(queueInfo)
    if (queue.has("queues")) {
      listBuffer ++= parseQueueArrayInfo(queue.getJSONObject("queues").getJSONArray("queue"), prefix + "  ")
    }
    listBuffer
  }

  case class Application(val id: String,
                         val user: String,
                         val name: String,
                         val applicationType: String,
                         val queue: String,
                         val progress: String,
                         val elapsedTime: String,
                         val allocatedMB: String,
                         val allocatedVCores: String,
                         val runningContainers: String,
                         val trackingUI: String,
                         val trackingUrl: String,
                         val diagnostics:String) extends ListableObject {
    override def toList: List[String] = List(this.id, this.applicationType, this.user,
      this.queue, this.name, this.progress, this.elapsedTime, this.allocatedMB, this.allocatedVCores, this.runningContainers)
  }

  object Application extends SchemaObject {
    override def getSchema: List[String] = List("application_Id", "Type",
      "User", "Queue", "Name", "Prgrs", "TimeUsd", "Mem(MB)", "VCr", "Cntnr")
  }

  case class MapReduceJobInfo(val mapProgress: String,
                              val mapsTotal: String,
                              val mapsPending: String,
                              val mapsRunning: String,
                              val mapsCompleted: String,
                              val failedMapAttempts: String,
                              val killedMapAttempts: String,
                              val reduceProgress: String,
                              val reducesTotal: String,
                              val reducesPending: String,
                              val reducesRunning: String,
                              val reducesCompleted: String,
                              val failedReduceAttempts: String,
                              val killedReduceAttempts: String) {
    def getSchema = {
      List("Kind", "% Complete", "Num Tasks", "Pending",
        "Running", "Complete", "Failed", "Killed")
    }

    def getSchemaData = {
      val result = new ListBuffer[Seq[String]]
      result.append(List("map", mapProgress + "%", mapsTotal, mapsPending, mapsRunning, mapsCompleted, failedMapAttempts, killedMapAttempts))
      result.append(List("reduce", reduceProgress + "%", reducesTotal, reducesPending, reducesRunning, reducesCompleted, failedReduceAttempts, killedReduceAttempts))
      result
    }
  }

  class MapReduceJob(id: String,
                     user: String,
                     name: String,
                     applicationType: String,
                     queue: String,
                     progress: String,
                     elapsedTime: String,
                     allocatedMB: String,
                     allocatedVCores: String,
                     runningContainers: String,
                     trackingUI: String,
                     trackingUrl: String,
                     diagnostics:String) extends Application(id, user, name, applicationType, queue, progress, elapsedTime,
    allocatedMB, allocatedVCores, runningContainers, trackingUI, trackingUrl,diagnostics) {
    var detailed: MapReduceJobInfo = null

    def getDetailedData = {
      detailed
    }

    def setDetailedData(jobInfo: MapReduceJobInfo) {
      detailed = jobInfo
    }
  }

  /**
   *
   * @param id task id
   * @param state NEW, SCHEDULED, RUNNING, SUCCEEDED, FAILED, KILL_WAIT, KILLED
   * @param _type MAP or REDUCE
   * @param successfulAttempt The the id of the last successful attempt
   * @param progress The progress of the task as a percent
   * @param startTime The time in which the task started (in ms since epoch)
   * @param finishTime The time in which the task finished (in ms since epoch)
   * @param elapsedTime The elapsed time since the application started (in ms)
   */
  case class MapReduceJobTask(val id: String,
                              val state: String,
                              val _type: String,
                              val successfulAttempt: String,
                              val progress: String,
                              val startTime: Long,
                              val finishTime: Long,
                              val elapsedTime: Long)

  def getMapReduceJobTasks(mrJob: MapReduceJob, tasktype: String, tasksate: String) = {
    require(mrJob != null)
    require(tasktype != null)
    require(tasksate != null)

    val mrJobId = "job_" + mrJob.id.replace("application_", "")
    val trackingUI = mrJob.trackingUI

    var taskUrl = mrJob.trackingUrl
    if (!taskUrl.endsWith("/"))
      taskUrl = taskUrl + "/"

    if (trackingUI.equals("History")) {
      taskUrl = Constants.JT_YARN_MR_HISTORY_ADDRESS + "/ws/v1/history/mapreduce/jobs/" + mrJobId + "/tasks"
    } else {
      taskUrl = taskUrl + "ws/v1/mapreduce/jobs/" + mrJobId + "/tasks"
    }

    logger.debug("Request for job tasks from " + taskUrl)

    val tasksString = loadRestfulAPI(taskUrl)
    logger.debug("Response " + tasksString)
    val tasksJsonObj: JSONObject = new JSONObject(tasksString)
    val listBuffer = ListBuffer[MapReduceJobTask]()
    if (tasksJsonObj.get("tasks") != JSONObject.NULL) {
      val tasks = tasksJsonObj.getJSONObject("tasks").getJSONArray("task")
      for (i <- 0 until tasks.length()) {
        val taskObj = tasks.getJSONObject(i)
        val mrJobTask = new MapReduceJobTask(taskObj.getString("id"), taskObj.getString("state"),
          taskObj.getString("type"), taskObj.getString("successfulAttempt"), taskObj.getDouble("progress"),
          taskObj.getLong("startTime"), taskObj.getLong("finishTime"), taskObj.getLong("elapsedTime"))

        if (mrJobTask._type.equalsIgnoreCase(tasktype)) {
          tasksate match {
            case "running" =>
              if (mrJobTask.state.equalsIgnoreCase("RUNNING"))
                listBuffer.append(mrJobTask)
            case "completed" =>
              if (mrJobTask.state.equalsIgnoreCase("SUCCEEDED"))
                listBuffer.append(mrJobTask)
            case "killed" =>
              if (mrJobTask.state.equalsIgnoreCase("KILLED"))
                listBuffer.append(mrJobTask)
            case "failed" =>
              if (mrJobTask.state.equalsIgnoreCase("FAILED"))
                listBuffer.append(mrJobTask)
            case _ =>
          }
        }
      }
    }
    listBuffer
  }

  /**
   *
   * @param id task id
   * @param state The state of the task attempt - valid values are: NEW, UNASSIGNED, ASSIGNED, RUNNING, COMMIT_PENDING, SUCCESS_CONTAINER_CLEANUP, SUCCEEDED, FAIL_CONTAINER_CLEANUP, FAIL_TASK_CLEANUP, FAILED, KILL_CONTAINER_CLEANUP, KILL_TASK_CLEANUP, KILLED
   * @param _type type of the task
   * @param assignedContainerId The container id this attempt is assigned to
   * @param nodeHttpAddress The http address of the node this task attempt ran on
   * @param diagnostics The diagnostics message
   * @param progress The progress of the task attempt as a percent
   * @param startTime The time in which the task attempt started (in ms since epoch)
   * @param finishTime The time in which the task attempt finished (in ms since epoch)
   * @param elapsedTime The elapsed time since the task attempt started (in ms)
   */
  case class MapReduceTaskAttempt(val id: String,
                                  val state: String,
                                  val _type: String,
                                  val assignedContainerId: String,
                                  val nodeHttpAddress: String,
                                  val diagnostics: String,
                                  val progress: String,
                                  val startTime: String,
                                  val finishTime: String,
                                  val elapsedTime: String)

  def getMapReduceTaskAttempts(mrJob: MapReduceJob, mrJobTask: MapReduceJobTask, attemptstatus: String) = {
    require(mrJob != null)
    require(mrJobTask != null)
    require(attemptstatus != null)

    val mrJobId = "job_" + mrJob.id.replace("application_", "")
    val trackingUI = mrJob.trackingUI

    var taskUrl = mrJob.trackingUrl
    if (!taskUrl.endsWith("/"))
      taskUrl = taskUrl + "/"

    if (trackingUI.equals("History")) {
      taskUrl = Constants.JT_YARN_MR_HISTORY_ADDRESS + "/ws/v1/history/mapreduce/jobs/" + mrJobId + "/tasks"
    } else {
      taskUrl = taskUrl + "ws/v1/mapreduce/jobs/" + mrJobId + "/tasks"
    }
    taskUrl = taskUrl + "/" + mrJobTask.id + "/attempts"

    logger.debug("Request for map-reduce job tasks attempt from " + taskUrl)

    val taskAttemptsString = loadRestfulAPI(taskUrl)
    val fixedAttemptString = taskAttemptsString.replace("\"type\":\"REDUCE\"","\"_type\":\"REDUCE\"")
    .replace("\"type\":\"MAP\"","\"_type\":\"MAP\"")
    logger.debug("Response " + fixedAttemptString)
    val taskAttemptsJsonObj: JSONObject = new JSONObject(fixedAttemptString)
    val listBuffer = ListBuffer[MapReduceTaskAttempt]()
    if (taskAttemptsJsonObj.get("taskAttempts") != JSONObject.NULL) {
      val taskAttempts = taskAttemptsJsonObj.getJSONObject("taskAttempts").getJSONArray("taskAttempt")
      for (i <- 0 until taskAttempts.length()) {
        val taskAttemptsObj = taskAttempts.getJSONObject(i)
        val mrJobTaskAttempt = new MapReduceTaskAttempt(taskAttemptsObj.getString("id"), taskAttemptsObj.getString("state"),
          taskAttemptsObj.getString("_type"), taskAttemptsObj.getString("assignedContainerId"), taskAttemptsObj.getString("nodeHttpAddress"),
          taskAttemptsObj.getString("diagnostics"), taskAttemptsObj.getDouble("progress"),
          taskAttemptsObj.getLong("startTime"), taskAttemptsObj.getLong("finishTime"), taskAttemptsObj.getLong("elapsedTime"))

        //use case match to handle one-multi mapping
        attemptstatus match {
          case "running" =>
            if (mrJobTaskAttempt.state.equalsIgnoreCase("running"))
              listBuffer.append(mrJobTaskAttempt)
          case "failed" =>
            if (mrJobTaskAttempt.state.equalsIgnoreCase("failed"))
              listBuffer.append(mrJobTaskAttempt)
          case "killed" =>
            if (mrJobTaskAttempt.state.equalsIgnoreCase("killed"))
              listBuffer.append(mrJobTaskAttempt)
          case "succeeded" =>
            if (mrJobTaskAttempt.state.equalsIgnoreCase("succeeded"))
              listBuffer.append(mrJobTaskAttempt)
          case _ =>
        }
      }
    }
    listBuffer
  }

  def fullFillMapReduce(mrJob: MapReduceJob) {
    var trackingUrl = mrJob.trackingUrl
    val trackingUI = mrJob.trackingUI
    if ("ApplicationMaster".equals(trackingUI)) {
      if (!trackingUrl.endsWith("/")) {
        trackingUrl = trackingUrl + "/"
      }
      trackingUrl = trackingUrl + "ws/v1/mapreduce/jobs/"
    } else {
      trackingUrl = Constants.JT_YARN_MR_HISTORY_ADDRESS + "/ws/v1/history/mapreduce/jobs/"
    }
    try {
      val jobUrl = trackingUrl + "job_" + mrJob.id;
      val jobString = loadRestfulAPI(jobUrl)
      val jobJsonObj: JSONObject = new JSONObject(jobString)
      if (jobJsonObj.get("job") != JSONObject.NULL) {
        val job = jobJsonObj.getJSONObject("job")
        if ("ApplicationMaster".equals(trackingUI)) {
          mrJob.setDetailedData(new MapReduceJobInfo(job.getDouble("mapProgress"),
            job.getInt("mapsTotal"), job.getInt("mapsPending"), job.getInt("mapsRunning"), job.getInt("mapsCompleted"),
            job.getInt("failedMapAttempts"), job.getInt("killedMapAttempts"),
            job.getDouble("reduceProgress"), job.getInt("reducesTotal"), job.getInt("reducesPending"),
            job.getInt("reducesRunning"), job.getInt("reducesCompleted"), job.getInt("failedReduceAttempts"), job.getInt("killedReduceAttempts")))
        } else {
          mrJob.setDetailedData(new MapReduceJobInfo(100.00,
            job.getInt("mapsTotal"), 0, 0, job.getInt("mapsCompleted"),
            job.getInt("failedMapAttempts"), job.getInt("killedMapAttempts"),
            100.00, job.getInt("reducesTotal"), 0,
            0, job.getInt("reducesCompleted"), job.getInt("failedReduceAttempts"), job.getInt("killedReduceAttempts")))
        }

      }
    } catch {
      case e: Exception => 
        System.err.println("Unable to get MR job information for job_" + mrJob.id + "  from " + trackingUrl)
    }

  }

  def getApplications(appId: String, user: String, queue: String = null, name: String = null) = {
    val listBuffer = ListBuffer[Application]()
    if (appId != null) {
      val app = getApplication(appId)
      if (app != null)
        listBuffer.append(app)
      listBuffer
    } else {
      var appUrl = Constants.JT_HTTP_BASE_URL + "/ws/v1/cluster/apps?states=running"
      if (user != null)
        appUrl += "&user=" + user
      if (queue != null)
        appUrl += "&queue=" + queue

      try {
        val appsString = loadRestfulAPI(appUrl)
        val appsJsonObj: JSONObject = new JSONObject(appsString)
        if (appsJsonObj.get("apps") != JSONObject.NULL) {
          val apps = appsJsonObj.getJSONObject("apps").getJSONArray("app")
          for (i <- 0 until apps.length()) {
            val appObj = apps.getJSONObject(i)
            if ("MAPREDUCE".equalsIgnoreCase(appObj.getString("applicationType"))) {
              listBuffer.append(new MapReduceJob(appObj.getString("id").replace("application_", ""),
                appObj.getString("user"), appObj.getString("name"),
                appObj.getString("applicationType"), appObj.getString("queue"),
                appObj.getLong("progress") + "%", TimeUtils.readableTime(appObj.getLong("elapsedTime")),
                appObj.getLong("allocatedMB"), appObj.getInt("allocatedVCores"),
                appObj.getInt("runningContainers"), appObj.getString("trackingUI"), appObj.getString("trackingUrl"),appObj.getString("diagnostics")))
            } else {
              listBuffer.append(new Application(appObj.getString("id").replace("application_", ""),
                appObj.getString("user"), appObj.getString("name"),
                appObj.getString("applicationType"), appObj.getString("queue"),
                appObj.getLong("progress") + "%", TimeUtils.readableTime(appObj.getLong("elapsedTime")),
                appObj.getLong("allocatedMB"), appObj.getInt("allocatedVCores"),
                appObj.getInt("runningContainers"), appObj.getString("trackingUI"), appObj.getString("trackingUrl"),appObj.getString("diagnostics")))
            }

          }
        }
      } catch {
        case e: Exception => System.err.println("Invalid parameters, please check!")
      }

      if (name == null)
        listBuffer
      else
        listBuffer.filter(p => p.name.toLowerCase().contains(name.toLowerCase()))
    }
  }

  def getApplication(appId: String) = {
    require(appId != null)
    val normAppId = normalizeId(appId)
    var appUrl = Constants.JT_HTTP_BASE_URL + "/ws/v1/cluster/apps/" + normAppId
    var result: Application = null
    try {
      val appsString = loadRestfulAPI(appUrl)
      val appsJsonObj: JSONObject = new JSONObject(appsString)
      if (appsJsonObj.get("app") != JSONObject.NULL) {
        val appObj = appsJsonObj.getJSONObject("app")
        if ("MAPREDUCE".equalsIgnoreCase(appObj.getString("applicationType"))) {
          result = new MapReduceJob(appObj.getString("id").replace("application_", ""),
            appObj.getString("user"), appObj.getString("name"),
            appObj.getString("applicationType"), appObj.getString("queue"),
            appObj.getLong("progress") + "%", TimeUtils.readableTime(appObj.getLong("elapsedTime")),
            appObj.getLong("allocatedMB"), appObj.getInt("allocatedVCores"),
            appObj.getInt("runningContainers"), appObj.getString("trackingUI"), appObj.getString("trackingUrl"),appObj.getString("diagnostics"))
        } else {
          result = new Application(appObj.getString("id").replace("application_", ""),
            appObj.getString("user"), appObj.getString("name"),
            appObj.getString("applicationType"), appObj.getString("queue"),
            appObj.getLong("progress") + "%", TimeUtils.readableTime(appObj.getLong("elapsedTime")),
            appObj.getLong("allocatedMB"), appObj.getInt("allocatedVCores"),
            appObj.getInt("runningContainers"), appObj.getString("trackingUI"), appObj.getString("trackingUrl"),appObj.getString("diagnostics"))
        }
      }
    } catch {
      case e: Exception => logger.debug("Unable to find Application with id: " + normAppId, e)
    }
    if (result == null)
      System.err.println("Unable to find Application with id: " + normAppId)
    result
  }

  def loadRestfulAPI(url: String) = {
    urlLoader(url)
  }

  def normalizeId(id: String) = {
    require(id != null)
    if (!id.startsWith("application_")) {
      if (!id.startsWith("job_"))
        "application_" + id
      else
        id.replace("job_", "application_")
    } else {
      id
    }
  }
}

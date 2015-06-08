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

import com.paypal.risk.hadoopjt.HjtArguments
import com.paypal.risk.hadoopjt.entity.HadoopV1Crawler.MapReduceV1Job
import com.paypal.risk.hadoopjt.util.{TablePrettyPrint, Constants}
import org.slf4j.LoggerFactory
import com.paypal.risk.hadoopjt.util.NetUtils

import scala.collection.mutable.ListBuffer

/**
 * @author Danick
 */
object HadoopV1JobDAO {

  val logger = LoggerFactory.getLogger(getClass())

  def listJobs(opts: HjtArguments) {

    val validJobs = HadoopV1Crawler.fullfillMapReduceJob(listJobs(opts.jobId, opts.userId, opts.queue, opts.name))
    if (validJobs.size == 1 && opts.tasktype != null) {
      //print the job information if it's tailing the log
      if (opts.logtype == "start=-2000")
        prettyPrint(validJobs, opts.raw)
      printLog(validJobs(0), opts.tasktype, opts.taskstate, opts.taskid, opts.attemptstatus, opts.logtype)
    } else
      prettyPrint(validJobs, opts.raw)
  }

  def listJobs(jobId: String, userId: String, queue: String, name: String) = {
    var filteredJobs = HadoopV1Crawler.parseAllJobs()
    if (queue != null) {
      filteredJobs = filteredJobs.filter(_.pool == queue)
    }

    if (userId != null) {
      filteredJobs = filteredJobs.filter(_.user == userId)
    }

    if (jobId != null) {
      filteredJobs = filteredJobs.filter(_.jobId == jobId)
    }

    if (name != null) {
      filteredJobs = filteredJobs.filter(_.name.toLowerCase().contains(name.toLowerCase()))
    }
    filteredJobs
  }

  def printLog(job: MapReduceV1Job, tasktype: String, tasksate: String, taskid: String, attemptstatus: String, logtype: String) {
    val jobId = job.jobId
    var taskUrl: String = null
    tasktype match {
      case "map" =>
        tasksate match {
          case "running" => if (job.mapTotal.toInt - job.mapComplete.toInt > 0) taskUrl = constructTasksPageUrl(jobId, tasktype, tasksate)
          case "completed" => if (job.mapComplete.toInt > 0) taskUrl = constructTasksPageUrl(jobId, tasktype, tasksate)
          case "killed" => if (job.mapKilled.toInt > 0) taskUrl = constructTasksPageUrl(jobId, tasktype, tasksate)
          case _ =>
        }
      case "reduce" =>
        tasksate match {
          case "running" => if (job.reduceTotal.toInt - job.reduceComlete.toInt > 0) taskUrl = constructTasksPageUrl(jobId, tasktype, tasksate)
          case "completed" => if (job.reduceComlete.toInt > 0) taskUrl = constructTasksPageUrl(jobId, tasktype, tasksate)
          case "killed" => if (job.reduceKilled.toInt > 0) taskUrl = constructTasksPageUrl(jobId, tasktype, tasksate)
          case _ =>
        }
      case _ =>
    }
    if (taskUrl != null) {
      val jobTaskList = HadoopV1Crawler.getJobTaskIds(taskUrl)

      if (jobTaskList != null) {
        var targetTaskId = jobTaskList(0)
        if (jobTaskList.size > taskid.toInt)
          targetTaskId = jobTaskList(taskid.toInt)

        var taskAttempIds = HadoopV1Crawler.getTaskAttemptIds(constructTaskUrl(targetTaskId), attemptstatus)
        if (taskAttempIds == null || taskAttempIds.size == 0) {
          attemptstatus match {
            case "running" => taskAttempIds = HadoopV1Crawler.getTaskAttemptIds(constructTaskUrl(targetTaskId), "succeeded")
            case "failed" => taskAttempIds = HadoopV1Crawler.getTaskAttemptIds(constructTaskUrl(targetTaskId), "succeeded")
            case "succeeded" => taskAttempIds = HadoopV1Crawler.getTaskAttemptIds(constructTaskUrl(targetTaskId), "running")
          }
        }
        if (taskAttempIds != null && taskAttempIds.size > 0) {
          val attempLogUrl = "http://" + taskAttempIds(0)._2 + ":50060/tasklog?attemptid=" + taskAttempIds(0)._1 + "&" + logtype
          val document = HadoopV1Crawler.loadHtmlDocument(attempLogUrl)
          val log: String = NetUtils.cleanDocument((document \ "body")(0).toString())
          val schema = List("Log of " + tasktype + " " + tasksate + " " + targetTaskId + " attempt:" + taskAttempIds(0)._1 + " attemptstatus:" + attemptstatus)
          val data = List(List(log))
          println()
          logtype match {
            case "start=-8193" => println(log)
            case "all=true" => println(log)
            case _ => TablePrettyPrint.printTable(schema, null); println(log)
          }
        }
      }
    } else
      println("No tasks of state " + tasksate + " in " + tasktype + " found!");

  }

  private def prettyPrint(jobs: Seq[MapReduceV1Job], raw: Boolean) {
    val schema = MapReduceV1Job.getSchema()
    val data = new ListBuffer[Seq[String]]
    for (job <- jobs) {
      data += job.toList()
    }
    if (raw)
      TablePrettyPrint.printTableRaw(schema, data)
    else
      TablePrettyPrint.printTable(schema, data)

    //print the detail table of map and reducers
    if (data.length == 1 && !raw) {
      println()
      println()
      val detailSchema = MapReduceV1Job.jobInfoSchema
      val detailData = jobs(0).jobInfoData
      TablePrettyPrint.printTable(detailSchema, detailData)
    }

  }

  def constructTasksPageUrl(jobid: String, tasktype: String, tasksate: String) = {
    Constants.JT_HTTP_BASE_URL + "/jobtasks.jsp?jobid=" + jobid + "&pagenum=1&type=" + tasktype + "&state=" + tasksate
  }

  def constructTaskUrl(tipid: String) = {
    Constants.JT_HTTP_BASE_URL + "/taskdetails.jsp?tipid=" + tipid
  }


}

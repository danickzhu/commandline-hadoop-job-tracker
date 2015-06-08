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
import com.paypal.risk.hadoopjt.entity.HadoopV2Crawler.{MapReduceTaskAttempt, MapReduceJob, Application}
import com.paypal.risk.hadoopjt.util.{NetUtils, TablePrettyPrint, Constants}
import scala.language.postfixOps
import sys.process._
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer

/**
 * @author Danick
 */
object HadoopV2JobDAO {

  val logger = LoggerFactory.getLogger(getClass())

  def listJobs(opts: HjtArguments) {
    val applications = HadoopV2Crawler.getApplications(opts.jobId, opts.userId, opts.queue, opts.name);
    val schema = Application.getSchema
    val data = new ListBuffer[Seq[String]]
    if (applications.size > 0) {
      for (job <- applications) {
        data += job.toList
      }
    }

    if (opts.raw)
      TablePrettyPrint.printTableRaw(schema, data)
    else
      TablePrettyPrint.printTable(schema, data)

    if (applications.size == 1) {

      println()
      println("Tracking url : " + applications(0).trackingUrl)
      println()

      if(applications(0).diagnostics != null && !"".equals(applications(0).diagnostics.trim()))
        println(applications(0).diagnostics)

      if (applications(0).isInstanceOf[com.paypal.risk.hadoopjt.entity.HadoopV2Crawler.MapReduceJob] && !opts.raw) {
        val mrJob = applications(0).asInstanceOf[com.paypal.risk.hadoopjt.entity.HadoopV2Crawler.MapReduceJob]
        HadoopV2Crawler.fullFillMapReduce(mrJob)
        if (mrJob.getDetailedData != null) {
          println()
          if (mrJob.trackingUI.equalsIgnoreCase("History")) {
            if (opts.tasktype != null || opts.logs) {
              yarnLog(mrJob)
            }
          } else {
            if (opts.tasktype == null || opts.logtype == "start=-2000")
              TablePrettyPrint.printTable(mrJob.getDetailedData.getSchema, mrJob.getDetailedData.getSchemaData)
            if (opts.tasktype != null)
              printV2Log(mrJob, opts.tasktype, opts.taskstate, opts.taskid, opts.attemptstatus, opts.logtype)
          }
        }
      } else {
        if (opts.tasktype != null || opts.logs) {
          yarnLog(applications(0))
        }
      }
    }
  }

  def yarnLog(app:Application): Unit ={
    require(app != null)
    val userId = System.getProperty("user.name")
    if (userId.equals(app.user)) {
      //directly invoke the yarn command
      logger.info("Invoking " + "yarn logs -applicationId application_" + app.id)
      "yarn logs -applicationId application_" + app.id !
    } else {
      logger.error(userId + " is not authorized to view logs of application application_" +
        app.id + "\n the user of the applications is " + app.user)
    }
  }

  def printV2Log(job: MapReduceJob, tasktype: String, tasksate: String, taskid: String, attemptstatus: String, logtype: String) {
    require(job != null, "Can not print logs for empty job")
    require(tasktype != null)

    val jobTasks = HadoopV2Crawler.getMapReduceJobTasks(job, tasktype, tasksate)

    if (jobTasks != null && jobTasks.size > 0) {

      //find attempts from all the tasks, in case some of the tasks have no attempts
      val allJobTaskAttempts = ListBuffer[MapReduceTaskAttempt]()
      for (taskAttempt <- jobTasks) {
        allJobTaskAttempts.appendAll(HadoopV2Crawler.getMapReduceTaskAttempts(job, taskAttempt, attemptstatus))
      }

      if (allJobTaskAttempts != null && allJobTaskAttempts.size > 0) {

        logger.debug("Got " + allJobTaskAttempts.size + " of task attemps of state " + attemptstatus)

        val firstJobTaskAttempt = allJobTaskAttempts(0)

        //prepare for log
        val containerId = firstJobTaskAttempt.assignedContainerId
        val nodeHttpAddress = firstJobTaskAttempt.nodeHttpAddress
        var logUrl = "http://" + nodeHttpAddress + "/node/containerlogs/" + containerId + "/syslog/syslog/?"
        var parameter = "start=-4096"
        logtype match {
          case "start=-8193" => parameter = "start=-8193"
          case "all=true" => parameter = "start=0"
          case _ =>
        }
        val document = NetUtils.loadHtmlDocumentAsXML(logUrl + parameter)
        logger.debug("Got log " + document)
        val tds = (document \ "body" \ "table" \ "tbody" \ "tr" \ "td").filter(td =>
          (td \ "@class").text == "content"
        )

        val log: String = NetUtils.cleanDocument((tds(0) \ "pre").toString())
        //TODO handle history tasks
        println(log)

      } else
        println("No tasks attempts of state " + attemptstatus + " of task state " + tasksate + " in " + tasktype + " found!");
    } else
      println("No tasks of state " + tasksate + " in " + tasktype + " found!");


  }

}

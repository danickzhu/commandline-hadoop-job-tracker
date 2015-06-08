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
package com.paypal.risk.hadoopjt

import org.slf4j.LoggerFactory

/**
 * Class to encapsulate the parameters from user input
 * @param opts
 */
class HjtArguments(opts: Seq[String]) {

  val logger = LoggerFactory.getLogger(getClass())

  /**
   * Whether to list running jobs
   */
  var listJobs: Boolean = false
  /**
   * Whether to list cluster status
   */
  var listCluster: Boolean = false
  /**
   * Job Id as a parameter<br/>
   * For Hadoop v1, it is the job id,<br/>
   * For Hadoop v2, it is application id or map reduce job id,
   * No need to add application_ or job_ prefix for the ids
   */
  var jobId: String = null
  /**
   * User Id as a parameter, default is the current user
   */
  var userId: String = null
  /**
   * Job queue as a parameter
   */
  var queue: String = null
  /**
   * Job priority as parameter <br/>
   * Currently only works for HADOOP V1
   */
  var priority: String = null
  /**
   * Whether to print the raw information, i.e. no need to print within ASCII table
   */
  var raw: Boolean = false
  /**
   * The characters of the job name contains
   */
  var name: String = null
  //for logs
  /**
   * MapReduce task type MAP/REDUCE
   */
  var tasktype: String = null
  /**
   * MapReduce task state PENDING/RUNNING/COMPLETED/KILLED
   */
  var taskstate: String = null
  /**
   * MapReduce task Id <br/>
   * Currently only works for HADOOP V1
   */
  var taskid: String = null
  /**
   * MapReduce task attempt status
   */
  var attemptstatus: String = null
  /**
   * How much of data of the log to print out<br/>
   * 4k, 8k, all
   */
  var logtype: String = null
  /**
   * Whether to print the debug information<br/>
   * Runtime will set the LOG mode to DEBUG
   */
  var debugMode :Boolean = false
  /**
   * Whether to display logs<br/>
   * Useful for HADOOP V2 applications
   */
  var logs : Boolean = false


  def parseOptions(opts: Seq[String]) {
    try {
      parseOption(opts)
      if (illegal)
        throw new IllegalArgumentException
    } catch {
      case ex: Exception => logger.debug("Error parsing the information",ex)
        printUsage()
        Main.exit(1)
    }

    //induce the list option
    if ((jobId != null && queue != null) || (jobId != null && priority != null) || ("all" equalsIgnoreCase queue)) {
      listJobs = false
    } else {
      if (jobId != null || userId != null || queue != null || name != null) {
        listJobs = true
      }
    }
  }

  def parseOption(opts: Seq[String]): Unit = opts match {
    case ("-l" | "--list") :: tail =>
      listJobs = true
      parseOption(tail)
    case ("-c" | "--cluster") :: tail =>
      listCluster = true
      parseOption(tail)
    case ("-r" | "--raw") :: tail =>
      raw = true
      parseOption(tail)
    case ("-j" | "--job") :: tail =>
      jobId = tail.head
      parseOption(tail.tail)
    case ("-u" | "--user") :: tail =>
      if (tail.isEmpty || tail.head.startsWith("-")) {
        userId = System.getProperty("user.name")
        parseOption(tail)
      } else {
        userId = tail.head
        parseOption(tail.tail)
      }
    case ("-q" | "--queue") :: tail =>
      if (tail.isEmpty || tail.head.startsWith("-")) {
        queue = "all"
        parseOption(tail)
      } else {
        queue = tail.head
        parseOption(tail.tail)
      }
    case ("-p" | "--priority") :: value :: tail =>
      priority = value
      parseOption(tail)
    case ("-n" | "--name") :: value :: tail =>
      name = value
      parseOption(tail)
    case "--type" :: value :: tail =>
      tasktype = value
      parseOption(tail)
    case "--state" :: value :: tail =>
      taskstate = value
      parseOption(tail)
    case "--taskid" :: value :: tail =>
      taskid = value
      parseOption(tail)
    case "--attemptstatus" :: value :: tail =>
      attemptstatus = value
      parseOption(tail)
    case "--logtype" :: value :: tail =>
      logtype = value
      parseOption(tail)
    case ("--debug") :: tail =>
      debugMode = true
      parseOption(tail)
    case ("--logs") :: tail =>
      logs = true
      parseOption(tail)
    case Nil =>
  }

  parseOptions(opts)
  postProcessOptions()

  def postProcessOptions() {
    if (tasktype != null) {
      tasktype match {
        case ("m" | "map") => tasktype = "map"; logs = true
        case ("r" | "reduce") => tasktype = "reduce"; logs = true
        case null => tasktype = "map"; logs = true
        case _ => jobId = null
      }

      taskid match {
        case null => taskid = "0"
        case _ => taskid = (taskid.toInt - 1) + ""
      }

      taskstate match {
        case ("r" | "running") => taskstate = "running"
        case ("c" | "completed") => taskstate = "completed"
        case ("k" | "killed") => taskstate = "killed"
        case ("f" | "failed"| "fail") => taskstate = "failed"
        case null => taskstate = "running"
        case _ => jobId = null
      }

      attemptstatus match {
        case ("r" | "running") => attemptstatus = "running"
        case ("s" | "success" | "succeeded") => attemptstatus = "succeeded"
        case ("f" | "fail" | "failed") => attemptstatus = "failed"
        case ("k" | "killed") => attemptstatus = "killed"
        case null => attemptstatus = "running"
        case _ => jobId = null
      }

      logtype match {
        case "4" => logtype = "start=-4097"
        case ("a" | "all") => logtype = "all=true"
        case "8" => logtype = "start=-8193"
        case null => logtype = "start=-2000"
        case _ => jobId = null
      }
    }
  }

  def illegal(): Boolean = {
    !listJobs && jobId == null && userId == null && queue == null && priority == null && name == null && !listCluster
  }

  override def toString() = {
    "listJobs:" + listJobs + "\njobId:" + jobId + "\nuserId:" + userId + "\nqueue:" + queue + "\npriority:" + priority
  }

  def printUsage() {
    val usage = """Tool to display job information from job tracker
<Bug report please send to guazhu@paypal.com>

Usage: hjt <command> <args>
    [-c|--cluster]                  list information of the cluster (highest priority)
    [-l|--list]                     list all the running jobs
    [-j|--job <jobid>]              specify jobid
    [-u|--user [userid]]            specify user name, default is current user name
    [-q|--queue [queue_name]]       specify queue name, no name specify will display all queue info
    [-r|--raw]                      output raw fields, divided by '|'
    [-n|--name <job_name>]          specify part of name
    [-j job_id --type [map|reduce] --state [running|completed|killed] --attemptstatus [running|success|fail] --logtype [4|8|all]
                                    pull log from job tracker
    [--logs]                        get logs for application specified
Example:
      hjt -l                        list all the running jobs
      hjt -j job_1333431534         list job job_1333431534 info
      hjt -u william                list the user william's all running jobs
      hjt -q default                list all the job in queue "default"
      hjt -j job_1333431534 -q default
                                    change job job_1333431534 to queue "default" (hadoop2 may not supported)
                """
    println(usage)
  }

}

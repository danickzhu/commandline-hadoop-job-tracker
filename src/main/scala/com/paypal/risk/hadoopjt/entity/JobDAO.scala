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


import org.slf4j.LoggerFactory

import sys.process._
import scala.language.postfixOps
import com.paypal.risk.hadoopjt.util._
import com.paypal.risk.hadoopjt.HjtArguments

/**
 * DAO for job information retrieving<br/>
 * For HADOOP V1, primarily the MapReduce job<br/>
 * For HADOOP V2, primarily the Applications
 *
 * @author Danick
 */
object JobDAO {

  val logger = LoggerFactory.getLogger(getClass())

  def listJobs(opts: HjtArguments) {
    if (Constants.HADOOP_VERSION == 2) {
      HadoopV2JobDAO.listJobs(opts)
    } else {
      HadoopV1JobDAO.listJobs(opts)
    }
  }


  def setJobAttributes(opts: HjtArguments) = {
    require(opts != null)

    val queue = opts.queue
    val jobId = opts.jobId
    val priority = opts.priority

    if (Constants.HADOOP_VERSION == 1) {
      if (queue != null) {
        NetUtils.curl(Constants.JT_HTTP_BASE_URL + "/scheduler?setPool=" + queue + "&jobid=" + jobId)
      }

      if (priority != null) {
        NetUtils.curl(Constants.JT_HTTP_BASE_URL + "/scheduler?setPriority=" + priority + "&jobid=" + jobId)
      }
    } else {
      val realJobId = HadoopV2Crawler.normalizeId(jobId)

      //delegate to yarn command
      "yarn application -movetoqueue " + realJobId + " -queue " + queue !
    }
  }


}

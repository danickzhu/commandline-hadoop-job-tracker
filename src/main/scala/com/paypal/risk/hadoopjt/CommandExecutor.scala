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

import com.paypal.risk.hadoopjt.entity.HadoopV1Crawler.{HadoopQueue, HadoopV1ClusterInfo}
import com.paypal.risk.hadoopjt.entity.HadoopV2Crawler.{SchedulerQueue, ClusterMetrics}
import com.paypal.risk.hadoopjt.entity._
import com.paypal.risk.hadoopjt.util.{Constants, TablePrettyPrint}

import scala.collection.mutable.ListBuffer

/**
 *
 * @param args arguments to execute
 * @author Danick
 */
class CommandExecutor(args: HjtArguments) {

  private val opts = args

  require(opts != null)

  def execute() {

    if (opts.listCluster) {
      listCluster()
    }
    else if (opts.listJobs) {
      JobDAO.listJobs(opts)
    } else {
      if (opts.jobId != null)
        JobDAO.setJobAttributes(opts)
      else if ("all" equalsIgnoreCase opts.queue) {
        listQueue()
      }
    }
  }

  private def listCluster(): Unit = {
    val clusterInfos = ClusterAndSchedulerDAO.getClusterInfo()
    if (clusterInfos != null && clusterInfos.size > 0) {
      var schemaOriginal: List[String] = null
      if (Constants.HADOOP_VERSION == 2) {
        schemaOriginal = ClusterMetrics.getSchema
      } else {
        schemaOriginal = HadoopV1ClusterInfo.getSchema()
      }

      val cluster = clusterInfos(0)
      val data = schemaOriginal.zip(cluster.toList).map { x =>
        List(x._1, x._2)
      }
      val schema = List("Metric", "Value")
      TablePrettyPrint.printTable(schema, data, opts.raw)
    }
  }

  private def listQueue(rawPrint:Boolean = false): Unit = {
    val queues = ClusterAndSchedulerDAO.getQueueInfo()
    if (queues != null && queues.size > 0) {
      var queueSchema: List[String] = null
      if (Constants.HADOOP_VERSION == 2) {
        queueSchema = SchedulerQueue.getSchema
      } else {
        queueSchema = HadoopQueue.getSchema()
      }

      val data = new ListBuffer[Seq[String]]
      for (q <- queues) {
        data += q.toList
      }
      TablePrettyPrint.printTable(queueSchema, data, rawPrint)
    }
  }

}

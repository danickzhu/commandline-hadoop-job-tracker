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

import HadoopV1Crawler._
import scala.collection.mutable.ListBuffer
import sys.process._
import scala.language.postfixOps
import com.paypal.risk.hadoopjt.util._
import EntityTraits._
import com.paypal.risk.hadoopjt.entity.HadoopV2Crawler.{SchedulerQueue, ClusterMetrics}
import com.paypal.risk.hadoopjt.JobTracker

/**
 * @author Danick
 */
object ClusterAndSchedulerDAO {

  def getClusterInfo() = {
    JobTracker.getInstance().getClusterInfo();
  }

  def getQueueInfo() = {
    JobTracker.getInstance().getQueuesInfo()
  }

  private def prettyPrint(queues: Seq[ListableObject], raw: Boolean) {
    var schemaOriginal: List[String] = null
    if (Constants.HADOOP_VERSION == 2) {
      schemaOriginal = SchedulerQueue.getSchema
    } else {
      schemaOriginal = HadoopQueue.getSchema()
    }
    val data = new ListBuffer[Seq[String]]
    for (q <- queues) {
      data += q.toList
    }
    if (raw)
      TablePrettyPrint.printTableRaw(schemaOriginal, data)
    else
      TablePrettyPrint.printTable(schemaOriginal, data)

  }

}

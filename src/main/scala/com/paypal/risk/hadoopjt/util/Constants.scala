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
package com.paypal.risk.hadoopjt.util

import org.slf4j.LoggerFactory
import scala.xml.XML
import scala.collection.mutable.Map

/**
 * @author Danick
 */
object Constants {

  val logger = LoggerFactory.getLogger(getClass())

  val hadoopConfigProperties: Map[String, String] = Map()

  val MAPRED_JT_HTTP_ADDRESS = "mapred.job.tracker.http.address"
  val MAPRED_JT_ADDRESS = "mapred.job.tracker"

  //hadoop v1 web pages
  val JT_JOB_TRACKER = "jobtracker.jsp"
  val JT_JOBQUEUE_DETAILS = "jobqueue_details.jsp"
  val JT_SCHEDULER = "scheduler"
  val JT_PARAM_QUEUENAME = "queueName"
  val JT_PARAM_JOBID = "jobid"
  val JT_PARAM_SETPOOL = "setPool"

  //hadoop v2 APIs
  val YARN_RM_HTTP_ADDRESS = "yarn.resourcemanager.webapp.address"
  val YARN_MR_HISTORY_ADDRESS = "mapreduce.jobhistory.webapp.address"

  def loadResource(xmlFile: String) {
    val resource = getClass().getClassLoader().getResource(xmlFile)
    if (resource != null) {
      logger.debug("load resource " + xmlFile)
      val resourceDoc = XML.load(resource)
      val properties = (resourceDoc \ "property")
      for (property <- properties) {
        val name = (property \ "name")(0).text
        val value = (property \ "value")(0).text
        hadoopConfigProperties.put(name, value)
      }
    } else {
      logger.debug("Can not load resource " + xmlFile)
    }
  }

  var JT_HTTP_BASE_URL: String = null
  var HADOOP_VERSION: Int = 0
  var JT_YARN_MR_HISTORY_ADDRESS:String = null
  def initConfiguration() = {
    loadResource("core-site.xml")
    loadResource("hdfs-site.xml")
    loadResource("yarn-site.xml")
    loadResource("mapred-site.xml")

    //firstly try with hadoop v2
    if (hadoopConfigProperties.contains(YARN_RM_HTTP_ADDRESS)) {
      HADOOP_VERSION = 2
      JT_HTTP_BASE_URL = "http://" + hadoopConfigProperties(YARN_RM_HTTP_ADDRESS)
      JT_YARN_MR_HISTORY_ADDRESS = "http://" + hadoopConfigProperties(YARN_MR_HISTORY_ADDRESS)
    } else {
      if (hadoopConfigProperties.contains(MAPRED_JT_HTTP_ADDRESS) && hadoopConfigProperties.contains(MAPRED_JT_ADDRESS)) {
        val port = hadoopConfigProperties(MAPRED_JT_HTTP_ADDRESS)
        val jobTrackerAdd = hadoopConfigProperties(MAPRED_JT_ADDRESS)
        HADOOP_VERSION = 1
        JT_HTTP_BASE_URL = "http://" + jobTrackerAdd.substring(0, jobTrackerAdd.indexOf(":")) + port.substring(port.indexOf(":"));
      } else {
        throw new RuntimeException("Unable to find job tracker location, make sure $HADOOP_CONFIG_DIR or $HADOOP_PREFIX or $HADOOP_HOME is properly configured")
      }
    }
  }

  initConfiguration()
}

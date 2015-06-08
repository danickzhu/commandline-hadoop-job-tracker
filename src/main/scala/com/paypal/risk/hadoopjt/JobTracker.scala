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

import com.paypal.risk.hadoopjt.entity.EntityTraits._
import com.paypal.risk.hadoopjt.entity._
import com.paypal.risk.hadoopjt.util._

/**
 * @author Danick
 */
trait JobTracker {

  //abstract methods
  def getClusterInfo(): Seq[ListableObject]

  def getQueuesInfo(): Seq[ListableObject]

}
object JobTracker {
  def getInstance(): JobTracker = {
    if (Constants.HADOOP_VERSION == 2) {
      HadoopV2Crawler
    } else if (Constants.HADOOP_VERSION == 1) {
      HadoopV1Crawler
    } else {
      throw new RuntimeException("Unable to determine the Hadoop version")
    }
  }
}

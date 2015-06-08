/*
 * Copyright 2015 Danick Zhu guazhu@ebay.com
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

import com.paypal.risk.hadoopjt.CommandExecutor
import com.paypal.risk.hadoopjt.util.NetUtilsTest
import org.scalatest.FunSuite

import org.json.JSONObject

import scala.xml.XML

class HadoopV1CrawlerTest extends FunSuite{

  test("Job details page should return elapsed time in 44 th element"){
    val time_used = HadoopV1Crawler.parseJobRunningTime(XML.loadString(NetUtilsTest.getTidyHtmlFromFile("jobdetails.jsp")))
    assert(time_used == "31hrs, 10mins, 38sec")
  }

  
}

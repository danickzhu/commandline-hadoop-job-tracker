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
package com.paypal.risk.hadoopjt.util

import org.jsoup.Jsoup
import scala.io.Source

/**
 * Helper class for testing
 */
object NetUtilsTest {

  def getTidyHtmlFromFile(path: String) = {
    if(path.endsWith("jobtracker.jsp")){
    	Jsoup.parse(Source.fromFile("src/test/resources/jobtracker.jsp").mkString).html
    }else if(path.endsWith("scheduler")){
    	Jsoup.parse(Source.fromFile("src/test/resources/scheduler").mkString).html
    }else if(path.endsWith("jobqueue_details.jsp")){
    	Jsoup.parse(Source.fromFile("src/test/resources/jobqueue_details.jsp").mkString).html
    }else{
    	Jsoup.parse(Source.fromFile("src/test/resources/jobdetails.jsp").mkString).html
    }
  }

  def mockHttpGet(url:String): String ={
    if (url.endsWith("ws/v1/cluster/apps")){
      Source.fromFile("src/test/resources/rm_applications.json").mkString
    }else{
      null
    }

  }


}

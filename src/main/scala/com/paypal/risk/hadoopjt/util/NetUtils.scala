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

import org.jsoup.Jsoup
import scala.io.Source
import org.slf4j.LoggerFactory

import scala.xml.XML

/**
 * Utility for operating http and network stuff
 * @author Danick
 */
object NetUtils {

  val logger = LoggerFactory.getLogger(getClass())

  def getTidyHtmlFromURL(url: String) = {
    logger.debug("Load url " + url)
    Jsoup.parse(Source.fromURL(url).mkString).html.replace("&nbsp;", " ")
  }

  def curl(url: String) {
    Source.fromURL(url)
  }

  def httpGet(url: String) = {
    Source.fromURL(url).mkString
  }

  def loadHtmlDocumentAsXML(url: String) = {
    //ugly remove the fisrt line
    val invalidString = "<!DOCTYPE html PUBLIC \"-//W3C//DTD HTML 4.01//EN\" \"http://www.w3.org/TR/html4/strict.dtd\">"
    XML.loadString(getTidyHtmlFromURL(url).replace(invalidString,""))
  }

  def cleanDocument(document: String) = {
    require(document != null)
    document.replaceAll("<(/?[^\\>]+)>", " ").replaceAll("(?m)^[ \t]*\r?\n", "").replaceAll("^[\\s\\t]+", "")
  }

}

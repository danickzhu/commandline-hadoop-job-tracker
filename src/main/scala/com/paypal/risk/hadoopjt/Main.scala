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

import org.apache.log4j.{Level, LogManager}
import org.slf4j.LoggerFactory
import com.paypal.risk.hadoopjt.util.Constants

/**
 * Entrance of the program <br/>
 * Mainly flow is
 * <ul>
 * <li>parse the command</li>
 * <li>execute the command</li>
 * </ul>
 * @author Danick
 */
object Main {

  val logger = LoggerFactory.getLogger(getClass())

  def main(args: Array[String]) {

    //parse options
    val options = new HjtArguments(args.toList)

    if (options.debugMode) {
      LogManager.getRootLogger.setLevel(Level.DEBUG)
    }

    //make sure configurations properly handled
    require(Constants.JT_HTTP_BASE_URL != null)
    logger.debug("Getting job query url " + Constants.JT_HTTP_BASE_URL)

    //execute the command
    new CommandExecutor(options).execute
  }

  def exit(errorCode: Int = 0) {
    System.exit(errorCode)
  }

}

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
package com.paypal.risk.hadoopjt

import org.scalatest.FunSuite

/**
 * Created by guazhu on 3/23/15.
 */
class HjtArgumentsTest extends FunSuite{

  test("HjtArguments should be able to parse all the parameters"){
    var hjt = new HjtArguments(List("-l"))
    assert(!hjt.illegal())
    assert(hjt.jobId == null)

    hjt = new HjtArguments(List("-u","-j","job_id","-q","default"))
    assert(hjt.userId == System.getProperty("user.name"))
    assert(hjt.queue == "default")
    assert(hjt.jobId == "job_id")
  }

}

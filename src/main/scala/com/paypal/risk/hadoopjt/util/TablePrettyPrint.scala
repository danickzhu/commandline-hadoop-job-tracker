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

import scala.collection.mutable.ListBuffer
import sys.process._
import scala.language.postfixOps

/**
 *
 * @author Danick
 *
 */
object TablePrettyPrint {

  val tableBorders = Map("top" -> '═', "top-mid" -> '╤', "top-left" -> '╔', "top-right" -> '╗',
    "bottom" -> '═', "bottom-mid" -> '╧', "bottom-left" -> '╚',
    "bottom-right" -> '╝', "left" -> '║', "left-mid" -> '╟', "mid" -> '─',
    "mid-mid" -> '┼', "right" -> '║', "right-mid" -> '╢', "middle" -> '│')


  def printTable(schema: Seq[String], data: Seq[Seq[String]], raw: Boolean = false) {
    var terminal_width: String = null;
    if (sys.env.contains("COLUMNS")) {
      terminal_width = sys.env("COLUMNS")
    }
    if (terminal_width == null || terminal_width.trim().length() == 0) {
      terminal_width = "tput cols" !!
    }
    if (raw)
      printTableRaw(schema, data)
    else
      printTableASCII(schema, data, terminal_width.stripLineEnd.toInt)
  }

  def printTableRaw(schema: Seq[String], data: Seq[Seq[String]], delimiter: String = "|") {
    require(schema != null, "Schema should not be null")
    println(schema.mkString(delimiter))
    if (data != null && data.length > 0) {
      data.foreach(x => println(x.mkString("|")))
    }
  }

  protected def printTableASCII(schema: Seq[String], data: Seq[Seq[String]], sceenWith: Int = 180, withColor: Boolean = false) {
    require(schema != null, "Schema should not be null")
    require(schema.length > 0, "Schema should not be null")
    require(sceenWith > schema.length * 2 + 1) //at least each column one character
    if (data != null && data.length > 0)
      require(schema.length == data(0).length, "Schema length is not equal to data length")

    val length = schema.length
    val column_for_border = length + 1
    val max_width_for_data = sceenWith - column_for_border

    //figure out max with for each column
    val max_width_each_column = ListBuffer.fill(length)(0)
    for (i <- 0 until length) {
      if (max_width_each_column(i) < schema(i).length()) {
        max_width_each_column(i) = schema(i).length()
      }
    }

    if (data != null && data.length > 0) {
      for (i <- 0 until data.length) {
        for (j <- 0 until length) {
          if (max_width_each_column(j) < data(i)(j).length()) {
            max_width_each_column(j) = data(i)(j).length()
          }
        }
      }
    }

    var current_data_width = max_width_each_column.sum
    var max_column_width = max_width_each_column.max
    var n_max_column_width = max_width_each_column.filter {
      _ == max_column_width
    }.length
    var width_gap = current_data_width - max_width_for_data
    var second_max_column_width = max_width_each_column.reduce((a, b) => {
      val max = a max b
      val min = a min b
      if (max >= max_column_width) min
      else max
    })
    // if all the same, all minus 1
    if (second_max_column_width == max_column_width) second_max_column_width = max_column_width - 1
    //check whether exceeds maximum
    //reduce from the biggest one
    //relative to the second largest

    while (width_gap > 0) {
      var n_minus = max_column_width - second_max_column_width
      if (width_gap < (max_column_width - second_max_column_width) * n_max_column_width) {
        //no need to cap max to second_max
        n_minus = if (width_gap % n_max_column_width > 0) width_gap / n_max_column_width + 1
        else
          width_gap / n_max_column_width
      }

      for (i <- 0 until max_width_each_column.length)
        if (max_width_each_column(i) == max_column_width)
          max_width_each_column(i) = max_column_width - n_minus

      current_data_width = max_width_each_column.sum
      max_column_width = max_width_each_column.max
      width_gap = current_data_width - max_width_for_data
      n_max_column_width = max_width_each_column.filter {
        _ == max_column_width
      }.length
      second_max_column_width = max_width_each_column.reduce((a, b) => {
        val max = a max b
        val min = a min b
        if (max >= max_column_width) min
        else max
      })
      // if all the same, minus 1
      if (second_max_column_width == max_column_width) second_max_column_width = max_column_width - 1
    }

    //print each line
    printHeader(max_width_each_column)
    printData(max_width_each_column, schema)
    if (data != null && data.length > 0) {
      printMidLiner(max_width_each_column)
      for (i <- 0 until data.length) {
        printData(max_width_each_column, data(i))
        if (i < data.length - 1) {
          printMidLiner(max_width_each_column)
        }
      }
    }
    printFooter(max_width_each_column)

  }

  private def printHeader(width: Seq[Int]) {
    print(tableBorders("top-left"))
    for (i <- 0 until width.length) {
      for (j <- 0 until width(i)) {
        print(tableBorders("top"))
      }
      if (i < width.length - 1) {
        print(tableBorders("top-mid"))
      }
    }
    print(tableBorders("top-right"))
    println()
  }

  private def printMidLiner(width: Seq[Int]) {
    print(tableBorders("left-mid"))
    for (i <- 0 until width.length) {
      for (j <- 0 until width(i)) {
        print(tableBorders("mid"))
      }
      if (i < width.length - 1) {
        print(tableBorders("mid-mid"))
      }
    }
    print(tableBorders("right-mid"))
    println()
  }

  private def printFooter(width: Seq[Int]) {
    print(tableBorders("bottom-left"))
    for (i <- 0 until width.length) {
      for (j <- 0 until width(i)) {
        print(tableBorders("bottom"))
      }
      if (i < width.length - 1) {
        print(tableBorders("bottom-mid"))
      }
    }
    print(tableBorders("bottom-right"))
    println()
  }

  private def printData(width: Seq[Int], data: Seq[String]) {
    //figure out how many rows need to print
    val n_rows_to_print = width.zip(data.map {
      _.length
    }).map(x => {
      if (x._2 % x._1 > 0) x._2 / x._1 + 1
      else x._2 / x._1
    }).max
    for (i <- 0 until n_rows_to_print) {
      printDataLine(width, data, i)
    }
  }

  private def printDataLine(width: Seq[Int], data: Seq[String], turn: Int = 0) {
    print(tableBorders("left"))
    for (i <- 0 until width.length) {
      val data_i_current_width = data(i).length() - width(i) * turn
      if (data_i_current_width < width(i)) {
        //no new data now
        if (data_i_current_width <= 0)
          for (j <- 0 until width(i))
            print(" ")
        else {
          print(data(i).substring(width(i) * turn))
          for (j <- data_i_current_width until width(i))
            print(" ")
        }
      } else {
        print(data(i).substring(width(i) * turn, width(i) * (turn + 1)))
      }
      if (i < width.length - 1) {
        print(tableBorders("middle"))
      }
    }
    print(tableBorders("right"))
    println()
  }

}

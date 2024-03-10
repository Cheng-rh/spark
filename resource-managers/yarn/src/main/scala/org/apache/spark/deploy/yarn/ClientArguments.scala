/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.deploy.yarn

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.internal.Logging

// TODO: Add code and support for ensuring that yarn resource 'tasks' are location aware !
private[spark] class ClientArguments(args: Array[String]) extends Logging {
  // 主jar包
  var userJar: String = null
  // 主类
  var userClass: String = null
  // 初始的python文件
  var primaryPyFile: String = null
  // 初始的R文件
  var primaryRFile: String = null
  // 主类的main参数
  var userArgs: ArrayBuffer[String] = new ArrayBuffer[String]()
  var verbose: Boolean = false

  // 解析参数
  parseArgs(args.toList)

  private def parseArgs(inputArgs: List[String]): Unit = {
    var args = inputArgs
    // 回填ClientArguments的成员变量:--jar、--class、--arg、eg
    while (!args.isEmpty) {
      args match {
        case ("--jar") :: value :: tail =>
          userJar = value
          args = tail

        case ("--class") :: value :: tail =>
          userClass = value
          args = tail

        case ("--primary-py-file") :: value :: tail =>
          primaryPyFile = value
          args = tail

        case ("--primary-r-file") :: value :: tail =>
          primaryRFile = value
          args = tail

        case ("--arg") :: value :: tail =>
          userArgs += value
          args = tail

        case ("--verbose" | "-v") :: tail =>
          verbose = true
          args = tail

        case Nil =>

        case _ =>
          throw new IllegalArgumentException(getUsageMessage(args))
      }
    }

    if (primaryPyFile != null && primaryRFile != null) {
      throw new IllegalArgumentException("Cannot have primary-py-file and primary-r-file" +
        " at the same time")
    }

    if (verbose) {
      logInfo(s"Parsed user args for YARN application: [${userArgs.mkString(" ")}]")
    }
  }

  private def getUsageMessage(unknownParam: List[String] = null): String = {
    val message = if (unknownParam != null) s"Unknown/unsupported param $unknownParam\n" else ""
    message +
      s"""
      |Usage: org.apache.spark.deploy.yarn.Client [options]
      |Options:
      |  --jar JAR_PATH           Path to your application's JAR file (required in YARN cluster
      |                           mode)
      |  --class CLASS_NAME       Name of your application's main class (required)
      |  --primary-py-file        A main Python file
      |  --primary-r-file         A main R file
      |  --arg ARG                Argument to be passed to your application's main class.
      |                           Multiple invocations are possible, each will be passed in order.
      |  --verbose, -v            Print additional debug output.
      """.stripMargin
  }
}

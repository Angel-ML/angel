/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in 
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */


package com.tencent.angel.jarvis.utils

import scala.collection.mutable

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.exception.InvalidParameterException

object ArgsParser {

  def parse(args: Array[String]): mutable.HashMap[String, String] = {
    val kvMap = mutable.HashMap[String, String]()

    var i: Int = 0
    while (i < args.length) {
      if (args(i).startsWith("-D")) {
        val seg = args(i).indexOf("=")
        if (seg > 0) {
          kvMap.put(args(i).substring(2, seg), args(i).substring(seg + 1))
        } else {
          throw new InvalidParameterException("invalid parameter " + args(i))
        }
      } else if (args(i).startsWith("--")) {
        val key = args(i).substring(2)
        i += 1
        if (i < args.length) {
          val value = args(i)
          kvMap.put(key, value)
        } else {
          throw new InvalidParameterException("there is no value for parameter " + key)
        }
      } else if (args(i).contains(":")) {
        val seg = args(i).indexOf(":")
        kvMap.put(args(i).substring(0, seg), args(i).substring(seg + 1))
      } else args(i) match {
        case "jar" =>
          if (i == args.length - 1) {
            throw new InvalidParameterException("there must be a jar file after jar commend")
          } else {
            i += 1
            kvMap.put(AngelConf.ANGEL_JOB_JAR, args(i))
          }
        case _ =>
          throw new InvalidParameterException("invalid parameter " + args(i))
      }
      i += 1
    }
    kvMap
  }
}
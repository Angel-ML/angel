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


package com.tencent.angel.ml.core.utils.paramsutils

object ParamParser {

  def parseMap(line: String, separator1: String = ",",
                      separator2: String = ":"): Map[String, String] = {
    if (line.isEmpty)
      return Map[String, String]()
    else
      line.trim
        .split(separator1)
        .map(_.split(separator2))
        .map{ case Array(k, v) => (k, v) }
        .toMap
  }

  def main(args: Array[String]): Unit = {
    val line = ""
    println(parseMap(line))
  }
}

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


package com.tencent.angel.spark.util

import org.apache.spark.SparkConf
import org.apache.hadoop.conf.Configuration

object ConfExchange {
  def spark2hadoop(spConf: SparkConf): Configuration = {
    val conf = new Configuration()
    spConf.getAll.foreach { case (key: String, value: String) =>
      conf.set(key, value)
    }

    conf
  }

  def hadoop2spark(hdConf: Configuration): SparkConf = {
    val conf = new SparkConf()
    val iter = hdConf.iterator()
    while (iter.hasNext) {
      val entry = iter.next()
      conf.set(entry.getKey, entry.getValue)
    }

    conf
  }
}

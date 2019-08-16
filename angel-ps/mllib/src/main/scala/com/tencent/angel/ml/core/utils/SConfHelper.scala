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
package com.tencent.angel.ml.core.utils

import java.io.File

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.mlcore.conf.SharedConf
import com.tencent.angel.mlcore.utils.JsonUtils
import org.apache.hadoop.conf.Configuration

trait SConfHelper {

  def initConf(conf: Configuration): SharedConf = {
    val sharedConf = new SharedConf

    // 1. parse json and update conf
    if (conf.get(AngelConf.ANGEL_ML_CONF) != null) {
      var jsonFileName = conf.get(AngelConf.ANGEL_ML_CONF)
      val validateFileName = if (new File(jsonFileName).exists()) {
        jsonFileName
      } else {
        val splits = jsonFileName.split(File.separator)
        jsonFileName = splits(splits.length - 1)

        val file = new File(jsonFileName)
        if (file.exists()) {
          jsonFileName
        } else {
          println("File not found! ")
          ""
        }
      }

      if (!validateFileName.isEmpty) {
        JsonUtils.parseAndUpdateJson(validateFileName, sharedConf, conf)
      }
    }

    // 2. add configure on Hadoop Configuration
    val iter = conf.iterator()
    while (iter.hasNext) {
      val entry = iter.next()
      val key = entry.getKey
      val value = entry.getValue

      if (key.startsWith("ml.") || key.startsWith("angel.")) {
        sharedConf.set(key, value)
      }
    }

    sharedConf
  }

}

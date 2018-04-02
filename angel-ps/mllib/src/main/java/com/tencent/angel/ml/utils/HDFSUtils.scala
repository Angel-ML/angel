/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package com.tencent.angel.ml.utils

import com.tencent.angel.ml.conf.MLConf
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.util.LineReader

object HDFSUtils {

  val LOG = LogFactory.getLog("HDFSUtils")

  def readFeatureNum(path: String, conf: Configuration): Int = {
    val maxdimPath = new Path(path)
    val fin = maxdimPath.getFileSystem(conf).open(maxdimPath)
    if (!maxdimPath.getFileSystem(conf).exists(maxdimPath)) {
      LOG.info("maxdimPath is null.")
    }

    val dim = new LineReader(fin)
    val line = new Text
    dim.readLine(line)
    val feaNum = Integer.valueOf(line.toString)
    dim.close()
    conf.set(MLConf.ML_FEATURE_INDEX_RANGE, String.valueOf(feaNum))
    feaNum
  }
}

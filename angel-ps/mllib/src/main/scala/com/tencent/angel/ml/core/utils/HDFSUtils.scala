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

import java.io.IOException

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.ml.math2.utils.DataBlock
import com.tencent.angel.mlcore.PredictResult
import com.tencent.angel.mlcore.conf.MLCoreConf
import com.tencent.angel.ml.predict.{PredictResult => OldPredictResult}
import com.tencent.angel.worker.task.TaskContext
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.Text
import org.apache.hadoop.util.LineReader

object HDFSUtils {

  val LOG = LogFactory.getLog("HDFSUtils")
  private val tmpPrefix = "_tmp."

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
    conf.set(MLCoreConf.ML_FEATURE_INDEX_RANGE, String.valueOf(feaNum))
    feaNum
  }

  @throws[IOException]
  def rename(tmpCombinePath: Path, outputPath: Path, fs: FileSystem): Unit = {
    if (fs.exists(outputPath)) fs.delete(outputPath, true)
    if (!fs.rename(tmpCombinePath, outputPath)) {
      throw new IOException("rename from " + tmpCombinePath + " to " + outputPath + " failed")
    }
  }

  @throws[IOException]
  def writeStorage(dataBlock: List[PredictResult], taskContext: TaskContext): Unit = {
    val outDir = taskContext.getConf.get(AngelConf.ANGEL_JOB_TMP_OUTPUT_PATH)
    val outPath = new Path(outDir, "predict")
    val fs = outPath.getFileSystem(taskContext.getConf)
    val outFileName = "task_" + taskContext.getTaskIndex
    val tmpOutFileName = tmpPrefix + outFileName
    val outFilePath = new Path(outPath, outFileName)
    val tmpOutFilePath = new Path(outPath, tmpOutFileName)
    if (fs.exists(tmpOutFilePath)) fs.delete(tmpOutFilePath, true)
    val output = fs.create(tmpOutFilePath)
    LOG.info("tmpOutFilePath=" + tmpOutFilePath)

    dataBlock.foreach{
      case resultItem: PredictResult if resultItem != null =>
        output.writeBytes(resultItem.getText + "\n")
      case _ =>
    }

    output.close()
    rename(tmpOutFilePath, outFilePath, fs)
    LOG.info("rename " + tmpOutFilePath + " to " + outFilePath)
  }

  @throws[IOException]
  def writeStorage(dataBlock: DataBlock[OldPredictResult], taskContext: TaskContext): Unit = {
    val outDir = taskContext.getConf.get(AngelConf.ANGEL_JOB_TMP_OUTPUT_PATH)
    val outPath = new Path(outDir, "predict")
    val fs = outPath.getFileSystem(taskContext.getConf)
    val outFileName = "task_" + taskContext.getTaskIndex
    val tmpOutFileName = tmpPrefix + outFileName
    val outFilePath = new Path(outPath, outFileName)
    val tmpOutFilePath = new Path(outPath, tmpOutFileName)
    if (fs.exists(tmpOutFilePath)) fs.delete(tmpOutFilePath, true)
    val output = fs.create(tmpOutFilePath)
    LOG.info("tmpOutFilePath=" + tmpOutFilePath)

    var rowId = 0
    val numRows = dataBlock.size()
    while (rowId < numRows) {
      val resultItem = dataBlock.loopingRead()
      if (resultItem != null) {
        output.writeBytes(resultItem.getText + "\n")
      }

      rowId += 1
    }

    output.close()
    rename(tmpOutFilePath, outFilePath, fs)
    LOG.info("rename " + tmpOutFilePath + " to " + outFilePath)
  }
}
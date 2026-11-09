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
package utils.io

import java.io.IOException

import org.apache.spark.{SparkConf, SparkContext, SparkEnv}
import org.apache.spark.storage.StorageLevel
import Conf._
import org.apache.hadoop.fs.{FileUtil, Path}
import org.apache.spark.graphx.PartitionStrategy


object Conf {
  private var checkpointInterval:Option[Int] = Some(Int.MaxValue)
  private var storageLevel:StorageLevel = StorageLevel.MEMORY_ONLY
  private var partitionStrategy:PartitionStrategy = PartitionStrategy.EdgePartition2D

  private val defaultFS = "spark.hadoop.fs.defaultFS"
  private val checkpointIntervalConf = "spark.ml.graph.checkpointInterval"
  private val storageLevelConf = "spark.ml.graph.storageLevel"
  private val partitionStrategyConf = "spark.ml.graph.partitionStrategy"
  private val checkpointRoot = "spark.ml.graph.checkpointRoot"


  def apply(conf: SparkConf): Conf = new Conf(conf)

  def getCheckpointInterval: Int = {
    checkpointInterval.fold(Int.MaxValue){default =>
      getConf.getOption(checkpointIntervalConf).map(_.toInt).getOrElse(default)
    }
  }

  def getStorageLevel: StorageLevel = {
    getConf.getOption(storageLevelConf)
      .map(StorageLevel.fromString)
      .getOrElse(storageLevel)
  }

  def getPartitionStrategy: PartitionStrategy = {
    getConf.getOption(partitionStrategyConf)
      .map(PartitionStrategy.fromString)
      .getOrElse(partitionStrategy)
  }

  def getOption(name: String): Option[String] = {
    getConf.getOption(s"spark.ml.graph.$name")
  }

  private def getConf:SparkConf = SparkEnv.get.conf

  def getStaging(sc: SparkContext): Option[Path] = {
    sc.getConf.getOption("spark.yarn.stagingDir")
      .orElse(sc.getConf.getOption(defaultFS))
      .orElse(Some("hdfs://hdfsCluster/tmp"))
      .map(new Path(_))
      .map(new Path(_, s".sparkStaging/${sc.getConf.getAppId}"))
  }
}

class Conf(conf: SparkConf) {

  def setDefaultCheckpointInterval(value: Int): this.type = {
    checkpointInterval = Some(value)
    this
  }

  def setDefaultStorageLevel(value: String): this.type = {
    storageLevel = StorageLevel.fromString(value)
    this
  }

  def setPartitionStrategy(value: String): this.type  = {
    partitionStrategy = PartitionStrategy.fromString(value)
    this
  }

  def createSparkContext: SparkContext = {
    val sc = new SparkContext(conf)
    if (!sc.isLocal) {
      conf.getOption(checkpointRoot)
        .orElse(getStaging(sc).map(new Path(_, "checkpoint").toString))
        .orElse {
          Log.withTimePrintln("no checkpoint dir, set checkpoint interval as +Infinity")
          checkpointInterval = None
          None
        }.foreach { cpRoot =>
        Log.withTimePrintln(s"set checkpoint dir: $cpRoot")
        sc.setCheckpointDir(cpRoot)
      }
    } else {
      sc.setLogLevel("WARN")
    }
    sc
  }

  private def existOrCreate(url:String){
    val checkPath = new Path(url)
    val fs = checkPath.getFileSystem(SparkContext.getOrCreate().hadoopConfiguration)
    if (!fs.exists(checkPath)) {
      try {
        fs.mkdirs(checkPath)
        FileUtil.chmod(url, "777")
      } catch {
        case e: IOException => Log.withTimePrintln(e.getMessage)
      }
    }
  }
}

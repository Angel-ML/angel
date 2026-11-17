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
package com.tencent.angel.spark.examples.local

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.graph.algo.rank.multiAttrHIndex.MultiAttrHIndex
import com.tencent.angel.spark.ml.graph.utils.GraphIO
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.concat_ws
import org.apache.spark.sql.types.StringType
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object MultiAttrHIndexLocalExample {


  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "local")
    val input = params.getOrElse("input", "data/bc/edge")
    val output = params.getOrElse("output", "model/multiattrhindex")
    val partitionNum = params.getOrElse("partitionNum","1").toInt
    val srcIndex = params.getOrElse("srcIndex", "0").toInt
    val dstIndex = params.getOrElse("dstIndex", "1").toInt
    val storageLevel = StorageLevel.fromString(params.getOrElse("storageLevel", "DISK_ONLY"))
    val batchSize = params.getOrElse("batchSize", "100").toInt
    val psPartitionNum = params.getOrElse("psPartitionNum", "1").toInt
    val useBalancePartition = params.getOrElse("useBalancePartition", "false").toBoolean
    val sep = params.getOrElse("sep", "space") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
    }
    val maxIteration = params.getOrElse("maxIteration", "1").toInt

    // params for node attrs files
    val nodeAttrPath = params.getOrElse("nodeAttrPath", null)
    val nodeIdInAttrPath = params.getOrElse("nodeIdInAttrPath", "0").toInt
    // if has only one kind of attribution, use a number of int to represent the column id
    // else, use "-" to denote the start and end column id
    // for example, "1-4" means column 1,2,3,4 are all the input attr columns
    val attrIndexRange = params.getOrElse("attrIndexRange", "1")
    // separation between columns in node attr files
    val sepInAttrPath = params.getOrElse("sepInAttrPath", "tab") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
    }

    start(mode)
    val hindex = new MultiAttrHIndex()
      .setPartitionNum(partitionNum)
      .setStorageLevel(storageLevel)
      .setPSPartitionNum(psPartitionNum)
      .setNodeAttrPath(nodeAttrPath)
      .setBatchSize(batchSize)
      .setUseBalancePartition(useBalancePartition)
      .setMaxIteration(maxIteration)

    val df = GraphIO.load(input, isWeighted = false, srcIndex, dstIndex, sep = sep)

    //collect app name and data size

    // read and set nodeAttr RDD
    hindex.setNodeAttr(readAttrs(nodeAttrPath, nodeIdInAttrPath, attrIndexRange, sepInAttrPath))

    val mapping = hindex.transform(df)
    GraphIO.save(mapping, output)
    stop()
  }

  def readAttrs(path: String, nodeId: Int, attrIndexRange: String, sep: String): RDD[(Long, Array[Float])] = {
    val attrIndexes = {
      val temp = attrIndexRange.split("-").map(_.toInt)
      if (temp.length == 1) {
        temp
      } else {
        assert(temp(0) < temp(1) && temp.length == 2, s"invalid attrIndexRange, $attrIndexRange")
        (temp(0) to temp(1)).toArray
      }
    }
    val selectAttrColumns = Array("node") ++ attrIndexes.map(x => "attr_" + x)
    val rawAttrs = GraphIO.loadNodeAttrs(path, nodeId, attrIndexes, sep)
    val attrs = rawAttrs.select(concat_ws(",", selectAttrColumns.map(rawAttrs.col(_)):_*).cast(StringType).as("attrs")).rdd
      .map(row => row.getString(0).split(","))
      .map(x => (x(0).toLong, x.tail.map(_.toFloat)))
    attrs
  }

  def start(mode: String = "local"): Unit = {
    val conf = new SparkConf()
    conf.setMaster(mode)
    conf.setAppName("hindex")
    conf.set(AngelConf.ANGEL_PSAGENT_UPDATE_SPLIT_ADAPTION_ENABLE, "false")
    // node attribute PSFs (GetNodeAttrs) only support range partitioning
    conf.set("spark.hadoop." + AngelConf.ANGEL_PS_ROUTER_TYPE, "range")
    val sc = new SparkContext(conf)
    //    sc.setLogLevel("WARN")
    //    sc.setCheckpointDir("cp")
    //    PSContext.getOrCreate(sc)
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }
}

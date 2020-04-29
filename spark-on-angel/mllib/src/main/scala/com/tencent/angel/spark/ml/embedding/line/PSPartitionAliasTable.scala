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

package com.tencent.angel.spark.ml.embedding.line

import java.util
import java.util.Random

import com.tencent.angel.PartitionKey
import com.tencent.angel.ml.matrix.psf.get.base._
import com.tencent.angel.spark.models.PSMatrix
import io.netty.buffer.ByteBuf

import scala.collection.JavaConversions._

/**
  * A alias table to choose a PS partition
  */
class PSPartitionAliasTable() {
  @volatile var prob: Array[Double] = _
  @volatile var alias: Array[Int] = _

  def init(psMatrix: PSMatrix) = {
    // Get weight sums for all ps partitions
    val weights = psMatrix.psfGet(new GetPSPartWeight(new GetParam(psMatrix.id))).asInstanceOf[GetPSPartWeightResult].getWeights
    for (i <- weights.indices) {
      println(s"index = $i, value=${weights(i)}")
    }

    // Build the alias table
    val aliasTable = AliasTableUtils.buildAliasTable(weights)
    prob = aliasTable._1
    alias = aliasTable._2
  }

  def batchSample(rand: Random, sampleNum: Int): Array[Int] = {
    AliasTableUtils.batchSample(rand, prob, alias, sampleNum)
  }
}

object PSPartitionAliasTable {
  var instance: PSPartitionAliasTable = _

  def get(psMatrix: PSMatrix): PSPartitionAliasTable = {
    PSPartitionAliasTable.getClass.synchronized {
      if (instance == null) {
        instance = new PSPartitionAliasTable()
        instance.init(psMatrix)
      }
      instance
    }
  }
}

/**
  * A PS function to get all ps partition weight sum
  *
  * @param param
  */
class GetPSPartWeight(param: GetParam) extends GetFunc(param) {

  def this() = this(null)

  /**
    * Partition get. This function is called on PS.
    *
    * @param partParam the partition parameter
    * @return the partition result
    */
  override def partitionGet(partParam: PartitionGetParam): PartitionGetResult = {
    val matrix = psContext.getMatrixStorageManager.getMatrix(partParam.getMatrixId)
    val part = matrix.getPartition(partParam.getPartKey.getPartitionId).asInstanceOf[EdgeAliasTablePartition]
    new PartGetPSPartWeightResult(partParam.getPartKey, part.weightSum())
  }

  /**
    * Merge the partition get results. This function is called on PSAgent.
    *
    * @param partResults the partition results
    * @return the merged result
    */
  override def merge(partResults: util.List[PartitionGetResult]): GetResult = {
    val weights = new Array[Double](partResults.size())
    partResults.foreach(e => {
      weights(e.asInstanceOf[PartGetPSPartWeightResult].partKey.getPartitionId)
        = e.asInstanceOf[PartGetPSPartWeightResult].partWeight
    })
    new GetPSPartWeightResult(weights)
  }
}

class GetPSPartWeightResult(weights: Array[Double]) extends GetResult {
  def getWeights: Array[Double] = weights
}

class PartGetPSPartWeightResult(var partKey: PartitionKey, var partWeight: Double) extends PartitionGetResult() {
  def this() = this(null, 0.0)

  /**
    * Serialize object to the Output stream.
    *
    * @param output the Netty ByteBuf
    */
  override def serialize(output: ByteBuf): Unit = {
    partKey.serialize(output)
    output.writeDouble(partWeight)
  }

  /**
    * Deserialize object from the input stream.
    *
    * @param input the input stream
    */
  override def deserialize(input: ByteBuf): Unit = {
    partKey = new PartitionKey()
    partKey.deserialize(input)
    partWeight = input.readDouble()
  }

  /**
    * Estimate serialized data size of the object, it used to ByteBuf allocation.
    *
    * @return int serialized data size of the object
    */
  override def bufferLen(): Int = {
    partKey.bufferLen() + 8
  }
}


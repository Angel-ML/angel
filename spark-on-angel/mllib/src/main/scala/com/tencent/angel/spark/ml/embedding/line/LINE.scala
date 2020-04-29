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

import com.tencent.angel.spark.ml.graph.params._
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}

/**
  * LINE
  * @param uid
  */
class LINE(override val uid: String) extends Transformer
  with HasEmbeddingDim with HasNegative with HasStepSize with HasOrder
  with HasCheckPointInterval with HasModelSaveInterval with HasSaveMeta with HasEpochNum with HasBatchSize
  with HasSrcNodeIdCol with HasDstNodeIdCol with HasNeedRemapping with HasSubSample with HasOutput
  with HasStorageLevel with HasPartitionNum with HasPSPartitionNum
  with HasWeightCol with HasIsWeighted with HasOldModelPath {

  def this() = this(Identifiable.randomUID("LINE"))

  /**
    * LINE PS model
    */
  @volatile var model:LINEModel = _

  override def transform(dataset: Dataset[_]): DataFrame = {
    if(${isWeighted}) {
      model = new LINEWithWightModel(dataset, ${embedding}, ${negative}, ${stepSize}, ${order},
        ${psPartitionNum}, ${batchSize}, ${epochNum}, ${partitionNum}, ${srcNodeIdCol}, ${dstNodeIdCol},
        ${weightCol}, ${remapping}, ${subSample}, ${output}, ${checkpointInterval}, ${saveModelInterval}, ${saveMeta}, ${oldModelPath})
    } else {
      model = new LINEModel(dataset, ${embedding}, ${negative}, ${stepSize}, ${order},
        ${psPartitionNum}, ${batchSize}, ${epochNum}, ${partitionNum}, ${srcNodeIdCol}, ${dstNodeIdCol},
        ${remapping}, ${subSample}, ${output}, ${checkpointInterval}, ${saveModelInterval}, ${saveMeta}, ${oldModelPath})
    }

    model.train()
    dataset.sparkSession.emptyDataFrame
  }

  def save(modelPathRoot: String, epoch: Int, saveMeta: Boolean): Unit = {
    model.save(modelPathRoot, epoch, saveMeta)
  }

  override def copy(extra: ParamMap): Transformer = ???

  override def transformSchema(schema: StructType): StructType = ???
}

case class Edge(src:Int, dst:Int) {
  override def hashCode() = {
    src * 13 + dst * 17
  }

  override def equals(obj: Any): Boolean = {
    obj.asInstanceOf[Edge].src == src && obj.asInstanceOf[Edge].dst == dst
  }
}

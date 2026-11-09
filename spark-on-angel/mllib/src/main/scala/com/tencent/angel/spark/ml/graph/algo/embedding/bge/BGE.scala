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
/**
 * Author: zhitaowang (zhitaowang@tecent.com)
 * Created At: 2021/03/01
 */

package com.tencent.angel.spark.ml.graph.algo.embedding.bge

import com.tencent.angel.spark.ml.graph.utils.params._
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}

/**
  * Bidirectional Graph Embedding based on LINE (BGE)
  * @param uid
  */
class BGE(override val uid: String) extends Transformer
  with HasEmbeddingDim with HasNegative with HasStepSize
  with HasCheckPointInterval with HasModelSaveInterval with HasSaveMeta with HasEpochNum with HasBatchSize
  with HasSrcNodeIdCol with HasDstNodeIdCol with HasNeedRemapping with HasSubSample with HasOutput
  with HasStorageLevel with HasPartitionNum with HasPSPartitionNum
  with HasWeightCol with HasIsWeighted with HasExtraInputEmbeddingPath
  with HasExtraOutputEmbeddingPath with HasUseBalancePartition {

  def this() = this(Identifiable.randomUID("BGE"))

  // val edgeCounter = new Object2IntOpenHashMap[Edge]()

  /**
    * BGE PS model
    */
  @volatile var model:BGEModel = null

  override def transform(dataset: Dataset[_]): DataFrame = {
    if($(isWeighted)) {
      model = new BGEWithWightModel(dataset, $(embedding), $(negative), $(stepSize),
        $(psPartitionNum), $(batchSize), $(epochNum), $(partitionNum), $(srcNodeIdCol), $(dstNodeIdCol),
        $(weightCol), $(remapping), $(subSample), $(output), $(checkpointInterval), $(saveModelInterval),
        $(extraInputEmbeddingPath), $(extraOutputEmbeddingPath), $(useBalancePartition))
    } else {
      model = new BGEModel(dataset, $(embedding), $(negative), $(stepSize),
        $(psPartitionNum), $(batchSize), $(epochNum), $(partitionNum), $(srcNodeIdCol), $(dstNodeIdCol),
        $(remapping), $(subSample), $(output), $(checkpointInterval), $(saveModelInterval), $(extraInputEmbeddingPath),
        $(extraOutputEmbeddingPath), $(useBalancePartition))
    }

    model.train()
    dataset.sparkSession.emptyDataFrame
  }

  def save(modelPathRoot: String, epoch: Int): Unit = {
    model.save(modelPathRoot, epoch)
  }

  override def copy(extra: ParamMap): Transformer = ???

  override def transformSchema(schema: StructType): StructType = ???
}

case class Edge(src:Int, tgt:Int) {
  override def hashCode() = {
    src * 13 + tgt * 17
  }

  override def equals(obj: Any): Boolean = {
    obj.asInstanceOf[Edge].src == src && obj.asInstanceOf[Edge].tgt == tgt
  }
}

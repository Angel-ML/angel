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


package com.tencent.angel.spark.ml.online_learning

import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.vector.{LongDoubleVector, Vector}
import com.tencent.angel.spark.models.vector.{PSVector, SparsePSVector, VectorCacheManager}
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

case class SparseLRModel(w: PSVector) {

  def sigmod(x: Double): Double = {
    1.0 / (1.0 + math.exp(-x))
  }

  def predict(instances: RDD[(Long, Vector)]): RDD[(Long, Double)] = {
    val result = instances.mapPartitions { iter =>
      val localX = w.toCache.pull()
      iter.map { case (id, feature) =>
        val margin = localX.dot(feature)
        Tuple2(id, sigmod(margin))
      }
    }
    VectorCacheManager.release(w)
    result
  }

  def predict(feat: Vector, localW: Vector): Double = {
    val score = localW.dot(feat)
    sigmod(score)
  }

  def save(modelPath: String): Unit = {
    val localX = w.pull
    val keyValues = localX.asInstanceOf[LongDoubleVector].getStorage
    val iter = keyValues.entryIterator()

    val modelArray = new Array[String](keyValues.size())
    var i = 0
    while (iter.hasNext) {
      val entry = iter.next()
      modelArray(i) = f"${entry.getLongKey} : ${entry.getDoubleValue}%.6f"
      i += 1
    }
    val spark = SparkSession.builder().getOrCreate()

    val path = new Path(modelPath)
    val fs = path.getFileSystem(spark.sparkContext.hadoopConfiguration)
    if (fs.exists(path)) fs.delete(path, true)

    spark.sparkContext.makeRDD(modelArray).saveAsTextFile(modelPath + "/data")
    val meta = Array(w.dimension.toString)
    spark.sparkContext.makeRDD(meta, 1).saveAsTextFile(modelPath + "/meta")
    println(s"save model finished")
  }

  def simpleInfo: String = {
    val localW = w.pull.asInstanceOf[LongDoubleVector]
    val nnz = localW.dim() - localW.numZeros()
    val wSparsity = nnz.toDouble / w.dimension

    s"weight sparsity : $wSparsity " +
      s"weight nnz: $nnz some index and value: ${localW.getStorage.getIndices.slice(0, 10).mkString("[", ",", "]")}" +
      s" => ${localW.getStorage.getValues.slice(0, 10).mkString("[", ",", "]")}"
  }
}

object SparseLRModel {
  def load(modelPath: String): SparseLRModel = {
    val spark = SparkSession.builder().getOrCreate()
    val dim = spark.sparkContext.textFile(modelPath + "/meta")
      .collect()(0).toLong

    val modelArray = spark.sparkContext.textFile(modelPath + "/data")
      .map { line =>
        val items = line.split(":")
        require(items.length == 2, "each line of model must be \"featureId:value\" format")
        Tuple2(items(0).trim.toLong, items(1).trim.toDouble)
      }.collect()
    val (key, value) = modelArray.unzip
    println(s"load data success, dim: $dim, model nnz: ${key.length}")
    val localX = VFactory.sparseLongKeyDoubleVector(dim, key, value)
    val psX = PSVector.sparse(localX.dim(), 20)
    psX.push(localX)
    println(s"load model successfully")
    SparseLRModel(psX)
  }
}

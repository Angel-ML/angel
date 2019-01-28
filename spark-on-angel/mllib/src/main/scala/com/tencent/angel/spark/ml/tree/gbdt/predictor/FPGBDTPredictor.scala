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


package com.tencent.angel.spark.ml.tree.gbdt.predictor

import com.tencent.angel.spark.ml.tree.common.TreeConf._
import com.tencent.angel.spark.ml.tree.data.Instance
import com.tencent.angel.spark.ml.tree.gbdt.tree.{GBTNode, GBTTree}
import com.tencent.angel.spark.ml.tree.objective.ObjectiveFactory
import com.tencent.angel.spark.ml.tree.util.DataLoader
import org.apache.hadoop.fs.Path
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

class FPGBDTPredictor extends Serializable {

  var forest: Seq[GBTTree] = _

  def loadModel(sc: SparkContext, modelPath: String): Unit = {
    forest = sc.objectFile[Seq[GBTTree]](modelPath).collect().head
    println(s"Reading model from $modelPath")
  }

  def predict(predictor: FPGBDTPredictor, instances: RDD[Instance]): RDD[(Long, Array[Float])] = {
    val bcPredictor = instances.sparkContext.broadcast(predictor)
    instances.map { instance =>
      (instance.label.toLong, bcPredictor.value.predictRaw(instance.feature))
    }
  }

  def predict(implicit sc: SparkContext, validPath: String, predPath: String): Unit = {
    println(s"Predicting dataset $validPath")
    val instances: RDD[Instance] = DataLoader.loadLibsvmDP(validPath, forest.head.getParam.numFeature).cache()
    //val labels = instances.map(_.label.toFloat).collect()
    val preds = predict(this, instances)
    instances.unpersist()

    //    println(s"Evaluating predictions")
    //    val evalMetrics = ObjectiveFactory.getEvalMetrics(
    //      sc.getConf.get(ML_EVAL_METRIC, DEFAULT_ML_EVAL_METRIC)
    //        .split(",").map(_.trim).filter(_.nonEmpty)
    //    )
    //    println(evalMetrics.map(evalMetric => {
    //      val kind = evalMetric.getKind
    //      val metric = evalMetric.eval(preds, labels)
    //      s"$kind[$metric]"
    //    }).mkString(", "))

    val path = new Path(predPath)
    val fs = path.getFileSystem(sc.hadoopConfiguration)
    if (fs.exists(path)) fs.delete(path, true)

    preds.map(pred => s"${pred._1}  ${pred._2.mkString(",")}").saveAsTextFile(predPath)
    println(s"Writing predictions to $predPath")
  }

  def predictRaw(vec: Vector): Array[Float] = {

    val param = forest.head.getParam
    val preds = Array.ofDim[Float](if (param.numClass == 2) 1 else param.numClass)
    forest.foreach(tree => {
      var node = tree.getRoot
      while (!node.isLeaf) {
        if (node.getSplitEntry.flowTo(vec) == 0)
          node = node.getLeftChild.asInstanceOf[GBTNode]
        else
          node = node.getRightChild.asInstanceOf[GBTNode]
      }
      if (param.numClass == 2) {
        preds(0) += node.getWeight * param.learningRate
      } else {
        val weights = node.getWeights
        for (k <- 0 until param.numClass)
          preds(k) += weights(k)
      }
    })
    preds
  }

  def probToClass(preds: Array[Float]): Int = {
    preds.length match {
      case 1 => if (preds.head > 0.5) 1 else 0
      case _ => preds.zipWithIndex.maxBy(_._1)._2 + 1
    }
  }

  def predict(vec: Vector): Int = {
    val preds = predictRaw(vec)
    probToClass(preds)
  }

  def predict(features: Array[Double]): Int = {
    val vec = Vectors.dense(features)
    val preds = predictRaw(vec)
    probToClass(preds)
  }

  def predict(dim: Int, indices: Array[Int], values: Array[Double]): Int = {
    val vec = Vectors.sparse(dim, indices, values)
    val preds = predictRaw(vec)
    probToClass(preds)
  }

}

object FPGBDTPredictor {

  def predict(predictor: FPGBDTPredictor, instances: RDD[Vector]): RDD[Array[Float]] = {
    val bcPredictor = instances.sparkContext.broadcast(predictor)
    instances.map(bcPredictor.value.predictRaw)
  }

  def main(args: Array[String]): Unit = {
    @transient val conf = new SparkConf()
    @transient implicit val sc = SparkContext.getOrCreate(conf)

    val modelPath = conf.get(ML_MODEL_PATH)
    val validPath = conf.get(ML_VALID_PATH)
    val predictPath = conf.get(ML_PREDICT_PATH)

    val predictor = new FPGBDTPredictor
    predictor.loadModel(sc, modelPath)
    predictor.predict(sc, validPath, predictPath)

    sc.stop
  }
}
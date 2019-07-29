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

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.tree.data.Instance
import com.tencent.angel.spark.ml.tree.gbdt.tree.{GBTNode, GBTTree}
import com.tencent.angel.spark.ml.tree.util.DataLoader
import org.apache.hadoop.fs.Path
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

class GBDTPredictor extends Serializable {

  var forest: Seq[GBTTree] = _

  def loadModel(sc: SparkContext, modelPath: String): Unit = {
    forest = sc.objectFile[Seq[GBTTree]](modelPath).collect().head
    println(s"Reading model from $modelPath")
  }

  def predict(predictor: GBDTPredictor, instances: RDD[Instance]): RDD[(Long, Int, Array[Float])] = {
    val bcPredictor = instances.sparkContext.broadcast(predictor)
    instances.map { instance =>
      val predProbs = bcPredictor.value.predictRaw(instance.feature)
      val predClass = bcPredictor.value.probToClass(predProbs)
      (instance.label.toLong, predClass, predProbs)
    }
  }

  def predict(implicit sc: SparkContext, predictPath: String, outputPath: String): Unit = {
    println(s"Predicting dataset: $predictPath")
    val instances: RDD[Instance] = DataLoader.loadLibsvmDP(predictPath, forest.head.getParam.numFeature).cache()
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

    val path = new Path(outputPath)
    val fs = path.getFileSystem(sc.hadoopConfiguration)
    if (fs.exists(path)) fs.delete(path, true)

    if (forest.head.getParam.isClassification)
      preds.map(pred => s"${pred._1} ${pred._2} ${pred._3.mkString(",")}").saveAsTextFile(outputPath)
    else
      preds.map(pred => s"${pred._1} ${pred._3.mkString(",")}").saveAsTextFile(outputPath)
    println(s"Writing predictions to $outputPath")
  }

  def predictRaw(vec: Vector): Array[Float] = {
    val param = forest.head.getParam
    require(param.numFeature == vec.size, s"feature dimension should be ${param.numFeature}")
    val preds = Array.ofDim[Float](if (param.numClass == 2) 1 else param.numClass)
    forest.zipWithIndex.foreach { case (tree: GBTTree, index: Int) =>
      var node = tree.getRoot
      while (!node.isLeaf) {
        if (node.getSplitEntry.flowTo(vec) == 0)
          node = node.getLeftChild.asInstanceOf[GBTNode]
        else
          node = node.getRightChild.asInstanceOf[GBTNode]
      }
      if (param.isRegression || param.numClass == 2) {
        preds(0) += node.getWeight * param.learningRate
      } else {
        if (param.isMultiClassMultiTree) {
          val curClass = index % param.numClass
          preds(curClass) += node.getWeight * param.learningRate
        } else {
          val weights = node.getWeights
          for (k <- 0 until param.numClass)
            preds(k) += weights(k)
        }
      }
    }
    preds
  }

  def probToClass(preds: Array[Float]): Int = {
    preds.length match {
      case 1 => if (preds.head > 0.5) 1 else 0
      case _ => preds.zipWithIndex.maxBy(_._1)._2
    }
  }

  def predictRaw(indices: Array[Int], values: Array[Double]): Array[Float] = {
    val param = forest.head.getParam
    require(param.numFeature > indices.last, s"feature dimension should be ${param.numFeature}")
    val vec = Vectors.sparse(param.numFeature, indices, values)
    predictRaw(vec)
  }

  def predictRaw(features: Array[Double]): Array[Float] = {
    val param = forest.head.getParam
    require(param.numFeature >= features.length, s"feature dimension should be ${param.numFeature}")
    val vec = Vectors.dense(features)
    predictRaw(vec)
  }

  def predict(vec: Vector): Int = {
    val param = forest.head.getParam
    require(param.numFeature == vec.size, s"feature dimension should be ${param.numFeature}")
    val preds = predictRaw(vec)
    probToClass(preds)
  }

  def predict(features: Array[Double]): Int = {
    val param = forest.head.getParam
    require(param.numFeature >= features.length, s"feature dimension should be ${param.numFeature}")
    val vec = Vectors.dense(features)
    val preds = predictRaw(vec)
    probToClass(preds)
  }

  def predict(indices: Array[Int], values: Array[Double]): Int = {
    val param = forest.head.getParam
    require(param.numFeature > indices.last, s"feature dimension should be ${param.numFeature}")
    val vec = Vectors.sparse(param.numFeature, indices, values)
    val preds = predictRaw(vec)
    probToClass(preds)
  }
}

object GBDTPredictor {

  def predict(predictor: GBDTPredictor, instances: RDD[Vector]): RDD[Array[Float]] = {
    val bcPredictor = instances.sparkContext.broadcast(predictor)
    instances.map(bcPredictor.value.predictRaw)
  }

  def main(args: Array[String]): Unit = {
    @transient val conf = new SparkConf()
    @transient implicit val sc = SparkContext.getOrCreate(conf)

    val params = ArgsUtil.parse(args)

    //val modelPath = params.getOrElse(TreeConf.ML_MODEL_PATH, "xxx")
    val modelPath = params.getOrElse(AngelConf.ANGEL_LOAD_MODEL_PATH, "xxx")

    //val predictPath = params.getOrElse(TreeConf.ML_PREDICT_PATH, "xxx")
    val predictPath = params.getOrElse(AngelConf.ANGEL_PREDICT_DATA_PATH, "xxx")

    //val outputPath = params.getOrElse(TreeConf.ML_OUTPUT_PATH, "xxx")
    val outputPath = params.getOrElse(AngelConf.ANGEL_PREDICT_PATH, "xxx")

    val predictor = new GBDTPredictor
    predictor.loadModel(sc, modelPath)
    predictor.predict(sc, predictPath, outputPath)

    sc.stop
  }
}
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

package com.tencent.angel.spark.ml.tree.gbdt.trainer

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.ml.core.conf.{MLConf, SharedConf}
import com.tencent.angel.spark.automl.tuner.config.{Configuration, ConfigurationSpace, EarlyStopping}
import com.tencent.angel.spark.automl.tuner.parameter.{ParamConfig, ParamParser}
import com.tencent.angel.spark.automl.tuner.solver.Solver
import com.tencent.angel.spark.automl.utils.AutoMLException
import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.tree.param.GBDTParam
import com.tencent.angel.spark.ml.tree.util.Maths
import com.tencent.angel.spark.ml.util.SparkUtils
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.util.Random

class AutoGBDTLearner(var tuneIter: Int = 20,
                      var minimize: Boolean = true,
                      var surrogate: String = "GaussianProcess",
                      var earlyStopping: EarlyStopping = null) {

  // Shared configuration with Angel-PS
  val conf = SharedConf.get()

  var solver: Solver = null

  def init(): this.type = {
    // set tuner params
    tuneIter = conf.getInt(MLConf.ML_AUTO_TUNER_ITER, MLConf.DEFAULT_ML_AUTO_TUNER_ITER)
    minimize = conf.getBoolean(MLConf.ML_AUTO_TUNER_MINIMIZE, MLConf.DEFAULT_ML_AUTO_TUNER_MINIMIZE)
    surrogate = conf.get(MLConf.ML_AUTO_TUNER_MODEL, MLConf.DEFAULT_ML_AUTO_TUNER_MODEL)

    // init solver
    val cs: ConfigurationSpace = new ConfigurationSpace("cs")
    solver = Solver(cs, minimize, surrogate)
    val config = conf.get(MLConf.ML_AUTO_TUNER_PARAMS)
    parseConfig(config)
    this
  }

  def train()(implicit sc: SparkContext): Unit = {

    val sparkConf = sc.getConf

    var param = new GBDTParam

    // spark conf
    val numExecutor = SparkUtils.getNumExecutors(sparkConf)
    val numCores = sparkConf.getInt("spark.executor.cores", 1)
    sparkConf.set("spark.task.cpus", numCores.toString)
    sparkConf.set("spark.locality.wait", "0")
    sparkConf.set("spark.memory.fraction", "0.7")
    sparkConf.set("spark.memory.storageFraction", "0.8")
    sparkConf.set("spark.task.maxFailures", "1")
    sparkConf.set("spark.yarn.maxAppAttempts", "1")
    sparkConf.set("spark.network.timeout", "1000")
    sparkConf.set("spark.executor.heartbeatInterval", "500")

    /** not tunerable params**/
    param.numWorker = numExecutor
    param.numThread = numCores
    // dataset conf
    param.taskType = SharedConf.get().get(MLConf.ML_GBDT_TASK_TYPE, MLConf.DEFAULT_ML_GBDT_TASK_TYPE)
    param.numClass = SharedConf.get().getInt(MLConf.ML_NUM_CLASS, MLConf.DEFAULT_ML_NUM_CLASS)
    param.numFeature = SharedConf.get().getInt(MLConf.ML_FEATURE_INDEX_RANGE, -1)
    // loss and metric
    param.lossFunc = SharedConf.get().get(MLConf.ML_GBDT_LOSS_FUNCTION, "binary:logistic")
    param.evalMetrics = SharedConf.get().get(MLConf.ML_GBDT_EVAL_METRIC, "error").split(",").map(_.trim).filter(_.nonEmpty)
    require(param.evalMetrics.size == 1, "only one eval metric is allowed under the tuning mode.")
    // task type
    param.taskType match {
      case "regression" =>
        require(param.lossFunc.equals("rmse") && param.evalMetrics(0).equals("rmse"),
          "loss function and metric of regression task should be rmse")
        param.numClass = 2
      case "classification" =>
        require(param.numClass >= 2, "number of labels should be larger than 2")
        param.multiStrategy = SharedConf.get().get("ml.gbdt.multi.class.strategy", "one-tree")
        if (param.isMultiClassMultiTree) param.lossFunc = "binary:logistic"
        param.multiGradCache = SharedConf.get().getBoolean("ml.gbdt.multi.class.grad.cache", true)
    }
    // conf switch
    param.histSubtraction = SharedConf.get().getBoolean(MLConf.ML_GBDT_HIST_SUBTRACTION, true)
    param.lighterChildFirst = SharedConf.get().getBoolean(MLConf.ML_GBDT_LIGHTER_CHILD_FIRST, true)
    param.fullHessian = SharedConf.get().getBoolean(MLConf.ML_GBDT_FULL_HESSIAN, false)

    println(s"Hyper-parameters:\n$param")

    val trainPath = SharedConf.get().get(AngelConf.ANGEL_TRAIN_DATA_PATH, "xxx")
    val validPath = SharedConf.get().get(AngelConf.ANGEL_VALIDATE_DATA_PATH, "xxx")
    val modelPath = SharedConf.get().get(AngelConf.ANGEL_SAVE_MODEL_PATH, "xxx")

    (0 until tuneIter).foreach{ iter =>
      println(s"==========Tuner Iteration[$iter]==========")
      val configs: Array[Configuration] = solver.suggest()
      for (config <- configs) {
        val paramMap: mutable.Map[String, Double] = new mutable.HashMap[String, Double]()
        for (paramType <- solver.getParamTypes) {
          setParam(paramType._1, paramType._2._2, config.get(paramType._1))
          paramMap += (paramType._1 -> config.get(paramType._1))
        }
        param = updateGBDTParam(param, paramMap)

        val trainer = new GBDTTrainer(param)
        trainer.initialize(trainPath, validPath)
        val (model, metrics) = trainer.train()
        trainer.save(model, modelPath)
        trainer.clear()

        solver.feed(config, metrics(0))
      }
    }

    val result: (Vector, Double) = solver.optimal()
    solver.stop()
    println(s"Best configuration ${result._1.toArray.mkString(",")}, best performance: ${result._2}")

  }

  def parseConfig(input: String): Unit = {
    val paramConfigs: Array[ParamConfig] = ParamParser.parse(input)
    paramConfigs.foreach{ config =>
      solver.addParam(config.getParamName, config.getParamType, config.getValueType,
        config.getParamRange, Random.nextInt())
    }
  }

  def setParam(name: String, vType: String, value: Double): Unit = {
    println(s"set param[$name] type[$vType] value[$value]")
    vType match {
      case "int" => SharedConf.get().setInt(name, value.toInt)
      case "long" => SharedConf.get().setLong(name, value.toLong)
      case "float" => SharedConf.get().setFloat(name, value.toFloat)
      case "double" => SharedConf.get().setDouble(name, value)
      case _ => throw new AutoMLException(s"unsupported value type $vType")
    }
  }

  def updateGBDTParam(param: GBDTParam, paramMap: mutable.Map[String, Double]): GBDTParam = {
    paramMap.foreach(println)

    case class ParseOp[T](op: String => T)
    implicit val toFloat = ParseOp[Double](_.toFloat)

    // major algo conf
    param.featSampleRatio = paramMap.getOrElse(MLConf.ML_GBDT_FEATURE_SAMPLE_RATIO,
      MLConf.DEFAULT_ML_GBDT_FEATURE_SAMPLE_RATIO).toFloat
    SharedConf.get().setFloat(MLConf.ML_GBDT_FEATURE_SAMPLE_RATIO, param.featSampleRatio)
    param.learningRate = paramMap.getOrElse(MLConf.ML_LEARN_RATE,
      MLConf.DEFAULT_ML_LEARN_RATE).toFloat
    SharedConf.get().setFloat(MLConf.ML_LEARN_RATE, param.learningRate)
    param.numSplit = paramMap.getOrElse(MLConf.ML_GBDT_SPLIT_NUM,
      MLConf.DEFAULT_ML_GBDT_SPLIT_NUM.toDouble).toInt
    SharedConf.get().setInt(MLConf.ML_GBDT_SPLIT_NUM, param.numSplit)
    param.numTree = paramMap.getOrElse(MLConf.ML_GBDT_TREE_NUM,
      MLConf.DEFAULT_ML_GBDT_TREE_NUM.toDouble).toInt
    if (param.isMultiClassMultiTree) param.numTree *= param.numClass
    SharedConf.get().setInt(MLConf.ML_GBDT_TREE_NUM, param.numTree)
    param.maxDepth = paramMap.getOrElse(MLConf.ML_GBDT_TREE_DEPTH,
      MLConf.DEFAULT_ML_GBDT_TREE_DEPTH.toDouble).toInt
    SharedConf.get().setInt(MLConf.ML_GBDT_TREE_DEPTH, param.maxDepth)
    val maxNodeNum = Maths.pow(2, param.maxDepth + 1) - 1
    param.maxNodeNum = paramMap.getOrElse(MLConf.ML_GBDT_MAX_NODE_NUM, 4096.toDouble).toInt min maxNodeNum
    SharedConf.get().setInt(MLConf.ML_GBDT_MAX_NODE_NUM, param.maxNodeNum)

    // less important algo conf
    param.minChildWeight = paramMap.getOrElse(MLConf.ML_GBDT_MIN_CHILD_WEIGHT,
      MLConf.DEFAULT_ML_GBDT_MIN_CHILD_WEIGHT).toFloat
    param.minNodeInstance = paramMap.getOrElse(MLConf.ML_GBDT_MIN_NODE_INSTANCE,
      MLConf.DEFAULT_ML_GBDT_MIN_NODE_INSTANCE.toDouble).toInt
    param.minSplitGain = paramMap.getOrElse(MLConf.ML_GBDT_MIN_SPLIT_GAIN,
      MLConf.DEFAULT_ML_GBDT_MIN_SPLIT_GAIN).toFloat
    param.regAlpha = paramMap.getOrElse(MLConf.ML_GBDT_REG_ALPHA,
      MLConf.DEFAULT_ML_GBDT_REG_ALPHA).toFloat
    param.regLambda = paramMap.getOrElse(MLConf.ML_GBDT_REG_LAMBDA,
      MLConf.DEFAULT_ML_GBDT_REG_LAMBDA).toFloat
    param.maxLeafWeight = paramMap.getOrElse(MLConf.ML_GBDT_MAX_LEAF_WEIGHT,
      MLConf.DEFAULT_ML_GBDT_MAX_LEAF_WEIGHT).toFloat

    param
  }

}

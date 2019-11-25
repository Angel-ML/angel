package com.tencent.angel.spark.examples.local

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.core.conf.{MLConf, SharedConf}
import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.tree.gbdt.predictor.GBDTPredictor
import com.tencent.angel.spark.ml.tree.gbdt.trainer.GBDTTrainer
import com.tencent.angel.spark.ml.tree.param.GBDTParam
import com.tencent.angel.spark.ml.tree.util.Maths
import org.apache.spark.{SparkConf, SparkContext}

object GBDTExample {

  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val actionType = params.getOrElse("actionType", "train")
    actionType match {
      case "train" => train(params)
      case "predict" => predict(params)
      case _ => throw new AngelException("Unsupported action type: " + actionType)
    }
  }

  def train(params: Map[String, String]): Unit = {
    @transient val conf = new SparkConf().setMaster("local").setAppName("gbdt-train")

    val param = new GBDTParam

    // spark conf
    require(!conf.getBoolean("spark.dynamicAllocation.enabled", false),
      "'spark.dynamicAllocation.enabled' should not be true")
    require(!conf.getBoolean("spark.blacklist.enabled", false),
      "'spark.blacklist.enabled' should not be true")
    val numExecutor = 1
    val numCores = 1
    param.numWorker = numExecutor
    param.numThread = numCores
    conf.set("spark.task.cpus", numCores.toString)
    conf.set("spark.locality.wait", "0")
    conf.set("spark.memory.fraction", "0.7")
    conf.set("spark.memory.storageFraction", "0.8")
    conf.set("spark.task.maxFailures", "1")
    conf.set("spark.yarn.maxAppAttempts", "1")
    conf.set("spark.network.timeout", "1000")
    conf.set("spark.executor.heartbeatInterval", "500")

    // dataset conf
    param.taskType = params.getOrElse(MLConf.ML_GBDT_TASK_TYPE, MLConf.DEFAULT_ML_GBDT_TASK_TYPE)
    param.numClass = params.getOrElse(MLConf.ML_NUM_CLASS, "2").toInt
    param.numFeature = params.getOrElse(MLConf.ML_FEATURE_INDEX_RANGE, "127").toInt + 1
    SharedConf.get().setInt(MLConf.ML_NUM_CLASS, param.numClass)
    SharedConf.get().setInt(MLConf.ML_FEATURE_INDEX_RANGE, param.numFeature)

    // loss and metric
    param.lossFunc = params.getOrElse(MLConf.ML_GBDT_LOSS_FUNCTION, "binary:logistic")
    param.evalMetrics = params.getOrElse(MLConf.ML_GBDT_EVAL_METRIC, "error").split(",").map(_.trim).filter(_.nonEmpty)
    SharedConf.get().set(MLConf.ML_GBDT_LOSS_FUNCTION, param.lossFunc)
    param.featureImportanceType = params.getOrElse(MLConf.ML_GBDT_FEAT_IMPORTANCE_TYPE, MLConf.DEFAULT_ML_GBDT_FEAT_IMPORTANCE_TYPE)

    param.taskType match {
      case "regression" =>
        require(param.lossFunc.equals("rmse") && param.evalMetrics(0).equals("rmse"),
          "loss function and metric of regression task should be rmse")
        param.numClass = 2
      case "classification" =>
        require(param.numClass >= 2, "number of labels should be larger than 2")
        param.multiStrategy = params.getOrElse("ml.gbdt.multi.class.strategy", "one-tree")
        if (param.isMultiClassMultiTree) param.lossFunc = "binary:logistic"
        param.multiGradCache = params.getOrElse("ml.gbdt.multi.class.grad.cache", "true").toBoolean
    }

    // major algo conf
    param.featSampleRatio = params.getOrElse(MLConf.ML_GBDT_FEATURE_SAMPLE_RATIO, "1.0").toFloat
    SharedConf.get().setFloat(MLConf.ML_GBDT_FEATURE_SAMPLE_RATIO, param.featSampleRatio)
    param.learningRate = params.getOrElse(MLConf.ML_LEARN_RATE, "0.1").toFloat
    SharedConf.get().setFloat(MLConf.ML_LEARN_RATE, param.learningRate)
    param.numSplit = params.getOrElse(MLConf.ML_GBDT_SPLIT_NUM, "10").toInt
    SharedConf.get().setInt(MLConf.ML_GBDT_SPLIT_NUM, param.numSplit)
    param.numTree = params.getOrElse(MLConf.ML_GBDT_TREE_NUM, "20").toInt
    if (param.isMultiClassMultiTree) param.numTree *= param.numClass
    SharedConf.get().setInt(MLConf.ML_GBDT_TREE_NUM, param.numTree)
    param.maxDepth = params.getOrElse(MLConf.ML_GBDT_TREE_DEPTH, "7").toInt
    SharedConf.get().setInt(MLConf.ML_GBDT_TREE_DEPTH, param.maxDepth)
    val maxNodeNum = Maths.pow(2, param.maxDepth + 1) - 1
    param.maxNodeNum = params.getOrElse(MLConf.ML_GBDT_MAX_NODE_NUM, "4096").toInt min maxNodeNum
    SharedConf.get().setInt(MLConf.ML_GBDT_MAX_NODE_NUM, param.maxNodeNum)

    // less important algo conf
    //param.histSubtraction = angelConf.getBoolean(MLConf.ML_GBDT_HIST_SUBTRACTION, MLConf.DEFAULT_ML_GBDT_HIST_SUBTRACTION)
    param.histSubtraction = params.getOrElse(MLConf.ML_GBDT_HIST_SUBTRACTION, "true").toBoolean
    param.lighterChildFirst = params.getOrElse(MLConf.ML_GBDT_LIGHTER_CHILD_FIRST, "true").toBoolean
    param.fullHessian = params.getOrElse(MLConf.ML_GBDT_FULL_HESSIAN, "false").toBoolean
    param.minChildWeight = params.getOrElse(MLConf.ML_GBDT_MIN_CHILD_WEIGHT, "0.01").toFloat
    param.minNodeInstance = params.getOrElse(MLConf.ML_GBDT_MIN_NODE_INSTANCE, "1024").toInt
    param.minSplitGain = params.getOrElse(MLConf.ML_GBDT_MIN_SPLIT_GAIN, "0.0").toFloat
    param.regAlpha = params.getOrElse(MLConf.ML_GBDT_REG_ALPHA, "0.0").toFloat
    param.regLambda = params.getOrElse(MLConf.ML_GBDT_REG_LAMBDA, "1.0").toFloat
    param.maxLeafWeight = params.getOrElse(MLConf.ML_GBDT_MAX_LEAF_WEIGHT, "0.0").toFloat

    println(s"Hyper-parameters:\n$param")

    val trainPath = params.getOrElse(AngelConf.ANGEL_TRAIN_DATA_PATH, "data/agaricus/agaricus_127d_train.libsvm")
    val validPath = params.getOrElse(AngelConf.ANGEL_VALIDATE_DATA_PATH, "data/agaricus/agaricus_127d_test.libsvm")
    val modelPath = params.getOrElse("modelPath", "file:///model/gbdt")

    @transient implicit val sc = new SparkContext(conf)

    val trainer = new GBDTTrainer(param)
    trainer.initialize(trainPath, validPath)
    val model = trainer.train()
    trainer.save(model, modelPath)
  }

  def predict(params: Map[String, String]): Unit = {
    @transient val conf = new SparkConf().setMaster("local").setAppName("gbdt-predict")
    @transient implicit val sc = SparkContext.getOrCreate(conf)

    val modelPath = params.getOrElse("modelPath", "file:///model/gbdt")
    val predictPath = params.getOrElse(AngelConf.ANGEL_PREDICT_DATA_PATH, "data/agaricus/agaricus_127d_test.libsvm")
    val outputPath = params.getOrElse(AngelConf.ANGEL_PREDICT_PATH,"file:///predict/gbdt")

    val predictor = new GBDTPredictor
    predictor.loadModel(sc, modelPath)
    predictor.predict(sc, predictPath, outputPath)
  }

}
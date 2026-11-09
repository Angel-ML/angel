package com.tencent.angel.spark.examples.local

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.core.conf.{MLConf, SharedConf}
import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.tree.kdtree.predictor.KDTPredictor
import com.tencent.angel.spark.ml.tree.kdtree.trainer.KDTTrainer
import com.tencent.angel.spark.ml.tree.param.KDTParam
import com.tencent.angel.spark.ml.tree.util.Maths
import org.apache.spark.{SparkConf, SparkContext}

object KDTreeExample {

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

    val param = new KDTParam

    // spark conf
    require(!conf.getBoolean("spark.dynamicAllocation.enabled", false),
      "'spark.dynamicAllocation.enabled' should not be true")
    require(!conf.getBoolean("spark.blacklist.enabled", false),
      "'spark.blacklist.enabled' should not be true")
    val numExecutor = 2
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
    param.numFeature = params.getOrElse(MLConf.ML_FEATURE_INDEX_RANGE, "8").toInt + 1
    SharedConf.get().setInt(MLConf.ML_FEATURE_INDEX_RANGE, param.numFeature)
    val dataType = params.getOrElse(MLConf.ML_DATA_INPUT_FORMAT, "dense")

    // major algo conf
    param.featSampleRatio = params.getOrElse(MLConf.ML_GBDT_FEATURE_SAMPLE_RATIO, "1.0").toFloat
    SharedConf.get().setFloat(MLConf.ML_GBDT_FEATURE_SAMPLE_RATIO, param.featSampleRatio)
    param.numSplit = params.getOrElse(MLConf.ML_GBDT_SPLIT_NUM, "7").toInt
    SharedConf.get().setInt(MLConf.ML_GBDT_SPLIT_NUM, param.numSplit)
    param.maxDepth = params.getOrElse(MLConf.ML_GBDT_TREE_DEPTH, "8").toInt
    SharedConf.get().setInt(MLConf.ML_GBDT_TREE_DEPTH, param.maxDepth)
    val maxNodeNum = Maths.pow(2, param.maxDepth + 1) - 1
    param.maxNodeNum = params.getOrElse(MLConf.ML_GBDT_MAX_NODE_NUM, "4096").toInt min maxNodeNum
    SharedConf.get().setInt(MLConf.ML_GBDT_MAX_NODE_NUM, param.maxNodeNum)

    // less important algo conf
    param.minNodeInstance = params.getOrElse(MLConf.ML_GBDT_MIN_NODE_INSTANCE, "1").toInt

    println(s"Hyper-parameters:\n$param")

    val trainPath = params.getOrElse(AngelConf.ANGEL_TRAIN_DATA_PATH, "data/gbt/kdtree_data_train.dense")
    val modelPath = params.getOrElse("modelPath", "file:///model/kdtree")
    val batchSize = params.getOrElse("batchSize", "100").toInt
    val numPsPartitions = params.getOrElse("numPsPartitions", "2").toInt

    @transient implicit val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val trainer = new KDTTrainer(param)
    trainer.initialize(trainPath, dataType)
    val (model, instanceInfo, dataset, labels) = trainer.train(batchSize, numPsPartitions)
    trainer.save(model, instanceInfo, dataset, labels, modelPath)
  }

  def predict(params: Map[String, String]): Unit = {
    @transient val conf = new SparkConf().setMaster("local").setAppName("kdtree-predict")
    @transient implicit val sc = SparkContext.getOrCreate(conf)
    sc.setLogLevel("ERROR")

    val modelPath = params.getOrElse("modelPath", "file:///model/kdtree")
    val nearestK = params.getOrElse("nearestK", "1").toInt
    val numPartitions = params.getOrElse("numPartitions", "2").toInt
    val isSimple = params.getOrElse("isSimple", "false").toBoolean
    val isGroupByCenter = params.getOrElse("isGroupByCenter", "false").toBoolean
    val predictPath = params.getOrElse(AngelConf.ANGEL_PREDICT_DATA_PATH, "data/gbt/kdtree_data_pred.dense")
    val outputPath = params.getOrElse(AngelConf.ANGEL_PREDICT_PATH,"file:///predict/kdtree")
    val dataType = params.getOrElse(MLConf.ML_DATA_INPUT_FORMAT, "dense")

    val predictor = new KDTPredictor
    predictor.loadModel(sc, modelPath)
    predictor.predict(sc, nearestK, numPartitions, predictPath, outputPath, isSimple, dataType, isGroupByCenter)

    sc.stop()
  }

}
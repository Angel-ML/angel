package com.tencent.angel.spark.ml

import com.tencent.angel.spark.ml.tree.common.TreeConf._
import com.tencent.angel.spark.ml.tree.gbdt.predictor.FPGBDTPredictor
import com.tencent.angel.spark.ml.tree.gbdt.trainer.SparkFPGBDTTrainer
import com.tencent.angel.spark.ml.tree.tree.param.GBDTParam
import com.tencent.angel.spark.ml.tree.util.Maths
import org.apache.hadoop.fs.Path

class GBDTTest extends PSFunSuite with SharedPSContext {
  private var trainer: SparkFPGBDTTrainer = _
  private var predictor: FPGBDTPredictor = _
  private var trainPath: String = _
  private var testPath: String = _
  private var modelPath: String = _
  private var predPath: String = _

  override def beforeAll(): Unit = {

    trainPath = "../../data/agaricus/agaricus_127d_train.libsvm"
    conf.set(ML_TRAIN_PATH, trainPath)
    testPath = "../../data/agaricus/agaricus_127d_train.libsvm"
    conf.set(ML_VALID_PATH, testPath)
    modelPath = "../../tmp/model"
    conf.set(ML_MODEL_PATH, modelPath)
    predPath = "../../tmp/pred"
    conf.set(ML_MODEL_PATH, modelPath)

    conf.set(ML_NUM_CLASS, "2")
    conf.set(ML_NUM_FEATURE, "149")
    conf.set(ML_NUM_WORKER, "1")
    conf.set(ML_LOSS_FUNCTION, "binary:logistic")
    conf.set(ML_EVAL_METRIC, "error,auc")
    conf.set(ML_LEARN_RATE, "0.1")
    conf.set(ML_GBDT_MAX_DEPTH, "3")

    super.beforeAll()

    val param = new GBDTParam
    param.numClass = conf.getInt(ML_NUM_CLASS, DEFAULT_ML_NUM_CLASS)
    param.numFeature = conf.get(ML_NUM_FEATURE).toInt
    param.featSampleRatio = conf.getDouble(ML_FEATURE_SAMPLE_RATIO, DEFAULT_ML_FEATURE_SAMPLE_RATIO).toFloat
    param.numWorker = conf.get(ML_NUM_WORKER).toInt
    param.numThread = conf.getInt(ML_NUM_THREAD, DEFAULT_ML_NUM_THREAD)
    param.lossFunc = conf.get(ML_LOSS_FUNCTION)
    param.evalMetrics = conf.get(ML_EVAL_METRIC, DEFAULT_ML_EVAL_METRIC).split(",").map(_.trim).filter(_.nonEmpty)
    param.learningRate = conf.getDouble(ML_LEARN_RATE, DEFAULT_ML_LEARN_RATE).toFloat
    param.histSubtraction = conf.getBoolean(ML_GBDT_HIST_SUBTRACTION, DEFAULT_ML_GBDT_HIST_SUBTRACTION)
    param.lighterChildFirst = conf.getBoolean(ML_GBDT_LIGHTER_CHILD_FIRST, DEFAULT_ML_GBDT_LIGHTER_CHILD_FIRST)
    param.fullHessian = conf.getBoolean(ML_GBDT_FULL_HESSIAN, DEFAULT_ML_GBDT_FULL_HESSIAN)
    param.numSplit = conf.getInt(ML_GBDT_SPLIT_NUM, DEFAULT_ML_GBDT_SPLIT_NUM)
    param.numTree = conf.getInt(ML_GBDT_TREE_NUM, DEFAULT_ML_GBDT_TREE_NUM)
    param.maxDepth = conf.getInt(ML_GBDT_MAX_DEPTH, DEFAULT_ML_GBDT_MAX_DEPTH)
    val maxNodeNum = Maths.pow(2, param.maxDepth + 1) - 1
    param.maxNodeNum = conf.getInt(ML_GBDT_MAX_NODE_NUM, maxNodeNum) min maxNodeNum
    param.minChildWeight = conf.getDouble(ML_GBDT_MIN_CHILD_WEIGHT, DEFAULT_ML_GBDT_MIN_CHILD_WEIGHT).toFloat
    param.minNodeInstance = conf.getInt(ML_GBDT_MIN_NODE_INSTANCE, DEFAULT_ML_GBDT_MIN_NODE_INSTANCE)
    param.minSplitGain = conf.getDouble(ML_GBDT_MIN_SPLIT_GAIN, DEFAULT_ML_GBDT_MIN_SPLIT_GAIN).toFloat
    param.regAlpha = conf.getDouble(ML_GBDT_REG_ALPHA, DEFAULT_ML_GBDT_REG_ALPHA).toFloat
    param.regLambda = conf.getDouble(ML_GBDT_REG_LAMBDA, DEFAULT_ML_GBDT_REG_LAMBDA).toFloat max 1.0f
    param.maxLeafWeight = conf.getDouble(ML_GBDT_MAX_LEAF_WEIGHT, DEFAULT_ML_GBDT_MAX_LEAF_WEIGHT).toFloat
    println(s"Hyper-parameters:\n$param")

    trainer = new SparkFPGBDTTrainer(param)
    predictor = new FPGBDTPredictor
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

  test("GBDT") {
    try {
      trainer.initialize(trainPath, testPath)(sc)
      val model = trainer.train()

      println(s"Model will be saved to $modelPath")
      trainer.save(model, modelPath)(sc)

      predictor.loadModel(sc, modelPath)
      predictor.predict(sc, testPath, predPath)
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      Array(modelPath, predPath).foreach { dir =>
        val path = new Path(dir)
        val fs = path.getFileSystem(sc.hadoopConfiguration)
        if (fs.exists(path)) fs.delete(path, true)
      }
    }
  }

}


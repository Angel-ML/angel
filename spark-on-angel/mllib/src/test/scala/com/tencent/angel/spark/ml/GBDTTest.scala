package com.tencent.angel.spark.ml

import com.tencent.angel.spark.ml.tree.gbdt.predictor.GBDTPredictor
import com.tencent.angel.spark.ml.tree.gbdt.trainer.GBDTTrainer
import com.tencent.angel.spark.ml.tree.param.GBDTParam
import com.tencent.angel.spark.ml.tree.util.Maths
import org.apache.hadoop.fs.Path

class GBDTTest extends PSFunSuite with SharedPSContext {
  private var trainer: GBDTTrainer = _
  private var predictor: GBDTPredictor = _
  private var trainPath: String = _
  private var testPath: String = _
  private var modelPath: String = _
  private var predPath: String = _

  override def beforeAll(): Unit = {

    super.beforeAll()

    trainPath = "../../data/agaricus/agaricus_127d_train.libsvm"
    testPath = "../../data/agaricus/agaricus_127d_train.libsvm"
    modelPath = "../../tmp/model"
    predPath = "../../tmp/pred"

    val param = new GBDTParam
    param.taskType = "classification"
    param.numClass = 2
    param.numFeature = 149
    param.featSampleRatio = 1.0f
    param.numWorker = 1
    param.numThread = 1
    param.lossFunc = "binary:logistic"
    param.evalMetrics = "error,auc".split(",").map(_.trim).filter(_.nonEmpty)
    param.learningRate = 0.1f
    param.histSubtraction = true
    param.lighterChildFirst = true
    param.fullHessian = false
    param.numSplit = 10
    param.numTree = 20
    param.maxDepth = 4
    val maxNodeNum = Maths.pow(2, param.maxDepth + 1) - 1
    param.maxNodeNum = maxNodeNum
    param.minChildWeight = 0.01f
    param.minNodeInstance = 10
    param.minSplitGain = 0.0f
    param.regAlpha = 0.0f
    param.regLambda = 0.1f
    param.maxLeafWeight = 0.0f
    println(s"Hyper-parameters:\n$param")

    trainer = new GBDTTrainer(param)
    predictor = new GBDTPredictor
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


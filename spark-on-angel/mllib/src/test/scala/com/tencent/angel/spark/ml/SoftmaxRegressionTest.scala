package com.tencent.angel.spark.ml

import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.core.conf.{MLConf, SharedConf}
import com.tencent.angel.ml.core.utils.DataParser
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.{ArgsUtil, GraphModel, OfflineLearner}
import org.apache.log4j.PropertyConfigurator
import org.apache.spark.{SparkConf, SparkContext}

object SoftmaxRegressionTest {

  def main(args: Array[String]): Unit = {
    PropertyConfigurator.configure("angel-ps/conf/log4j.properties")
    val params = ArgsUtil.parse(args)

    val input = params.getOrElse("input", "./data/protein/protein_357d_train.libsvm")
    val actionType = params.getOrElse("actionType", "train")

    // build SharedConf with params
    SharedConf.get()
    SharedConf.addMap(params)
    SharedConf.get().set(MLConf.ML_MODEL_TYPE, RowType.T_FLOAT_DENSE.toString)
    SharedConf.get().setInt(MLConf.ML_FEATURE_INDEX_RANGE, 357)
    SharedConf.get().setDouble(MLConf.ML_LEARN_RATE, 0.5)
    SharedConf.get().set(MLConf.ML_DATA_INPUT_FORMAT, "libsvm")
    SharedConf.get().setInt(MLConf.ML_EPOCH_NUM, 10)
    SharedConf.get().setDouble(MLConf.ML_VALIDATE_RATIO, 0.0)
    SharedConf.get().setDouble(MLConf.ML_REG_L2, 0.002)
    SharedConf.get().setDouble(MLConf.ML_BATCH_SAMPLE_RATIO, 0.2)
    SharedConf.get().setInt(MLConf.ML_NUM_CLASS, 3)

    val className = "com.tencent.angel.spark.ml.classification.SoftmaxRegression"
    val model = GraphModel(className)
    val learner = new OfflineLearner()

    // load data
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("Softmax Test")
    conf.set("spark.ps.model", "LOCAL")
    conf.set("spark.ps.jars", "")
    conf.set("spark.ps.instances", "1")
    conf.set("spark.ps.cores", "1")
    conf.set("spark.ps.log.level", "INFO")

    val sc = new SparkContext(conf)
    val parser = DataParser(SharedConf.get())
    val data = sc.textFile(input).map(f => parser.parse(f))

    PSContext.getOrCreate(sc)

    actionType match {
      case "train" =>
        learner.train(data, model)
      case "predict" =>
        model.load("")
        learner.predict(data, model)
      case _ =>
        throw new AngelException("actionType should be train or predict")
    }

    PSContext.stop()
    sc.stop()
  }

}

package com.tencent.angel.spark.ml

import com.tencent.angel.RunningMode
import com.tencent.angel.conf.AngelConf
import com.tencent.angel.ml.core.conf.{MLConf, SharedConf}
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.spark.automl.tuner.config.EarlyStopping
import com.tencent.angel.spark.ml.classification.LogisticRegression
import com.tencent.angel.spark.ml.core.AutoOfflineLearner


class AutoLRTestGP extends PSFunSuite with SharedPSContext {
  private var learner: AutoOfflineLearner = _
  private var input: String = _
  private var dim: Int = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    input = "../data/census/census_148d_train.libsvm"


    // build SharedConf with params
    SharedConf.get()
    SharedConf.get().set(MLConf.ML_MODEL_TYPE, RowType.T_FLOAT_DENSE.toString)
    SharedConf.get().setInt(MLConf.ML_FEATURE_INDEX_RANGE, 149)
    SharedConf.get().setDouble(MLConf.ML_LEARN_RATE, 0.5)
    SharedConf.get().setDouble(MLConf.ML_LEARN_DECAY, 0.2)
    SharedConf.get().set(MLConf.ML_DATA_INPUT_FORMAT, "libsvm")
    SharedConf.get().setInt(MLConf.ML_EPOCH_NUM, 50)
    SharedConf.get().setInt(MLConf.ML_DECAY_INTERVALS, 10)
    SharedConf.get().setDouble(MLConf.ML_VALIDATE_RATIO, 0.1)
    SharedConf.get().setDouble(MLConf.ML_REG_L2, 0.0)
    SharedConf.get().setDouble(MLConf.ML_BATCH_SAMPLE_RATIO, 0.2)
    dim = SharedConf.indexRange.toInt

    SharedConf.get().set(AngelConf.ANGEL_RUNNING_MODE, RunningMode.ANGEL_PS.toString)
    val Earlystop = new EarlyStopping(patience = 5, minimize = true, min_delta = 0.01)

    learner = new AutoOfflineLearner(25,true, surrogate = "GP", Earlystop)
    learner.addParam("continuous", "double", MLConf.ML_LEARN_RATE, "[0.1:1:100]")
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

  test("LogisticRegression") {
    val model = new LogisticRegression
    learner.train(input, "", "", dim, model)
  }

}

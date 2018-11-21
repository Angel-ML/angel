package com.tencent.angel.spark.ml

import com.tencent.angel.ml.core.conf.{MLConf, SharedConf}
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.spark.ml.classification._
import com.tencent.angel.spark.ml.core.OfflineLearner

class NetworkSuite extends PSFunSuite with SharedPSContext {
  private var learner: OfflineLearner = _
  private var input: String = _
  private var dim: Int = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    learner = new OfflineLearner

    input = "../../data/census/census_148d_train.libsvm"

    // build SharedConf with params
    SharedConf.get()
    SharedConf.get().set(MLConf.ML_MODEL_TYPE, RowType.T_FLOAT_DENSE.toString)
    SharedConf.get().setInt(MLConf.ML_FEATURE_INDEX_RANGE, 149)
    SharedConf.get().setDouble(MLConf.ML_LEARN_RATE, 0.5)
    SharedConf.get().set(MLConf.ML_DATA_INPUT_FORMAT, "libsvm")
    SharedConf.get().setInt(MLConf.ML_EPOCH_NUM, 200)
    SharedConf.get().setDouble(MLConf.ML_VALIDATE_RATIO, 0.0)
    SharedConf.get().setDouble(MLConf.ML_REG_L2, 0.0)
    SharedConf.get().setDouble(MLConf.ML_BATCH_SAMPLE_RATIO, 0.2)
    dim = SharedConf.indexRange.toInt
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

  test("LogisticRegression") {
    val model = new LogisticRegression
    learner.train(input, "", "", dim, model)
  }

  test("SupportVectorMachine") {
    val model = new SupportVectorMachine
    learner.train(input, "", "", dim, model)
  }

  test("FactorizationMachine") {
    val model = new FactorizationMachine
    learner.train(input, "", "", dim, model)
  }

  test("MixedLogisticRegression") {
    val model = new MixedLogisticRegression
    learner.train(input, "", "", dim, model)
  }

  test("SoftmaxRegression") {
    SharedConf.get().setInt(MLConf.ML_NUM_CLASS, 3)
    val model = new SoftmaxRegression
    learner.train(input, "", "", dim, model)
  }

  test("DeepFM") {
    SharedConf.get().setLong(MLConf.ML_FIELD_NUM, 13)
    SharedConf.get().setLong(MLConf.ML_RANK_NUM, 5)

    val model = new DeepFM
    learner.train(input, "", "", dim, model)
  }

  test("WideAndDeep") {
    SharedConf.get().setLong(MLConf.ML_FIELD_NUM, 13)
    SharedConf.get().setLong(MLConf.ML_RANK_NUM, 5)
    val model = new WideAndDeep
    learner.train(input, "", "", dim, model)
  }
}

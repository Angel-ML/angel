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

package com.tencent.angel.spark.ml

import com.tencent.angel.RunningMode
import com.tencent.angel.conf.AngelConf
import com.tencent.angel.ml.core.conf.{MLConf, SharedConf}
import com.tencent.angel.spark.ml.tree.gbdt.trainer.AutoGBDTLearner
import org.apache.spark.SparkContext

class AutoGBDTTest extends PSFunSuite with SharedPSContext {
  private var learner: AutoGBDTLearner = _
  private var input: String = _
  private var modelPath: String = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    input = "../../data/census/census_148d_train.libsvm"
    modelPath = "../../tmp/gbdt"

    // build SharedConf with params
    SharedConf.get()

    SharedConf.get().set(AngelConf.ANGEL_TRAIN_DATA_PATH, input)
    SharedConf.get().set(AngelConf.ANGEL_VALIDATE_DATA_PATH, input)
    SharedConf.get().set(AngelConf.ANGEL_SAVE_MODEL_PATH, modelPath)

    SharedConf.get().set(MLConf.ML_GBDT_TASK_TYPE, "classification")
    SharedConf.get().setInt(MLConf.ML_NUM_CLASS, 2)
    SharedConf.get().setInt(MLConf.ML_FEATURE_INDEX_RANGE, 149)
    SharedConf.get().set(MLConf.ML_GBDT_LOSS_FUNCTION, "binary:logistic")
    SharedConf.get().set(MLConf.ML_GBDT_EVAL_METRIC, "auc")

    SharedConf.get().setDouble(MLConf.ML_GBDT_FEATURE_SAMPLE_RATIO, 1.0)
    SharedConf.get().setDouble(MLConf.ML_LEARN_RATE, 0.1)
    SharedConf.get().setInt(MLConf.ML_GBDT_SPLIT_NUM, 10)
    SharedConf.get().setInt(MLConf.ML_GBDT_TREE_NUM, 10)
    SharedConf.get().setInt(MLConf.ML_GBDT_TREE_DEPTH, 3)

    SharedConf.get().setInt(MLConf.ML_AUTO_TUNER_ITER, 5)
    SharedConf.get().setBoolean(MLConf.ML_AUTO_TUNER_MINIMIZE, false)
    SharedConf.get().set(MLConf.ML_AUTO_TUNER_MODEL, "GaussianProcess")
    SharedConf.get().set(MLConf.ML_AUTO_TUNER_PARAMS,
      "ml.learn.rate|C|double|0.1:1:100#ml.gbdt.tree.num|D|int|2,4,6,8,10")

    SharedConf.get().set(AngelConf.ANGEL_RUNNING_MODE, RunningMode.ANGEL_PS.toString)

    learner = new AutoGBDTLearner().init()
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

  test("AutoGBDT") {
    learner.train()(sc)
  }

}

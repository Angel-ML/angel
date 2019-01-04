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


package com.tencent.angel.spark.automl

object AutoConf {

  object Preprocess {

    val ML_DATA_INPUT_FORMAT = "ml.data.format"
    val DEFAULT_ML_DATA_INPUT_FORMAT = "libsvm"

    val ML_DATA_SPLITOR = "ml.data.splitor"
    val DEFAULT_ML_DATA_SPLITOR = "\\s+"

    val INPUT_TYPE = "ml.input.type"
    val DEFAULT_INPUT_TYPE = "normal"

    val SAMPLE_RATE = "ml.sample.rate"
    val DEFAULT_SAMPLE_RATE = "1.0"

    val IMBALANCE_SAMPLE = "ml.imbalance.sample"
    val DEFAULT_IMBALANCE_SAMPLE = "false"

    val HAS_DISCRETER = "ml.has.discreter"
    val DEFAULT_HAS_DISCRETER = "false"

    val HAS_ONEHOTER = "ml.has.onehoter"
    val DEFAULT_HAS_ONEHOTER = "false"

    val HAS_MINMAXSCALAR = "ml.has.minmaxscalar"
    val DEFAULT_HAS_MINMAXSCALAR = "true"

    val HAS_STANDARDSCALAR = "ml.has.standardscalar"
    val DEFAULT_HAS_STANDARDSCALAR = "false"

  }

}

class AutoConf {}

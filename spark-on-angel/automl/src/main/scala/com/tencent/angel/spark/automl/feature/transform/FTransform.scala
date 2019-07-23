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


package com.tencent.angel.spark.automl.feature.transform

import com.tencent.angel.spark.automl.AutoConf
import com.tencent.angel.spark.automl.utils.ArgsUtil

class FTransform {

  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val master = params.getOrElse("master", "yarn")
    val deploy = params.getOrElse("deploy-mode", "cluster")
    val input = params.getOrElse("input", "")
    val inputSeparator = params.getOrElse(AutoConf.Preprocess.ML_DATA_SPLITOR,
      AutoConf.Preprocess.DEFAULT_ML_DATA_SPLITOR)
    val inputFormat = params.getOrElse(AutoConf.Preprocess.ML_DATA_INPUT_FORMAT,
      AutoConf.Preprocess.DEFAULT_ML_DATA_INPUT_FORMAT)
    val inputType = params.getOrElse(AutoConf.Preprocess.INPUT_TYPE,
      AutoConf.Preprocess.DEFAULT_INPUT_TYPE)
    val hasDiscreter = params.getOrElse(AutoConf.Preprocess.HAS_DISCRETER,
      AutoConf.Preprocess.DEFAULT_HAS_DISCRETER)
    val hasOnehoter = params.getOrElse(AutoConf.Preprocess.HAS_ONEHOTER,
      AutoConf.Preprocess.DEFAULT_HAS_ONEHOTER)
    val hasMinMaxScalar = params.getOrElse(AutoConf.Preprocess.HAS_MINMAXSCALAR,
      AutoConf.Preprocess.DEFAULT_HAS_MINMAXSCALAR)
    val hasStdScalar = params.getOrElse(AutoConf.Preprocess.HAS_STANDARDSCALAR,
      AutoConf.Preprocess.DEFAULT_HAS_STANDARDSCALAR)
  }


}

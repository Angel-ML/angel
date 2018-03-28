/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package com.tencent.angel.spark.ml

import org.apache.spark.mllib.linalg.VectorUDT
import org.apache.spark.sql.types._

package object util {

  val MAX_INT_STR = Int.MaxValue.toString

  val TDW_PREFIX = "tdw://"
  val HDFS_PREFIX = "hdfs://"

  val SPLIT_SEPARATOR = "\\s+|,"
  val KEY_VALUE_SEP = ":"

  object StorageType {
    val LOCAL = "LOCAL"
    val HDFS = "HDFS"
    val TDW = "TDW"
  }

  object DataFormat {
    val DENSE = "dense"
    val SPARSE = "sparse"
  }

  object DataType {
    val UNLABELED = "unlabeled"
    val LABELED = "labeled"
  }

  object DFStruct {
    val FEATURE = "feature"
    val LABEL = "label"
    val PREDICT = "predict"
    val PROB = "prob"
    val ID = "id"
  }

  object ActionType {
    val TRAIN = "train"
    val PREDICT = "predict"
    val SERVING= "serving"
  }

  object ParamKeys {
    // input and output param
    val INPUT = "input"
    val BEGIN_COL = "beginCol"
    val END_COL = "endCol"
    val LABEL_COL = "labelCol"
    val SCORE_COL = "scoreCol"
    val OUTPUT = "output"
    val PARTITION_NUM = "partitionNum"
    val SAMPLE_RATE = "sampleRate"

    // algorithm param
    val MAX_ITER = "maxIter"
    val TOL = "tol"
    val THRESHOLD = "threshold"
    val ELASTIC_NET = "elasticNet"
    val REG_PARAM = "regParam"
    val STEP_SIZE = "stepSize"
    val MINI_BATCH_FRACTION = "miniBatchFraction"
    val METRIC_NAME = "metricName"

    // system param keys
    val MODE = "mode"
    val ACTION_TYPE = "actionType"
    val MODEL_PATH = "modelPath"
    val TEST_SET = "validateSet"
  }

  val UNLABELED_ST = StructType(
    StructField(DFStruct.FEATURE, new VectorUDT(), false) ::
      Nil)

  val LABELED_ST = StructType(
    StructField(DFStruct.FEATURE, new VectorUDT(), false) ::
      StructField(DFStruct.LABEL, DoubleType, false) ::
      Nil)

  val LIBSVM_ST = StructType(
    StructField(DFStruct.LABEL, DoubleType, false) ::
      StructField(DFStruct.FEATURE, new VectorUDT(), false) ::
      Nil)

  val LIBSVM_PREDICT_ST = StructType(
    StructField(DFStruct.ID, StringType, false) ::
      StructField(DFStruct.FEATURE, new VectorUDT(), false) ::
      Nil)

  val ONE_HOT_INSTANCE_ST = StructType(
    StructField(DFStruct.LABEL, StringType, false) ::
      StructField(DFStruct.FEATURE, ArrayType(LongType, containsNull = false), false) ::
      Nil)

}

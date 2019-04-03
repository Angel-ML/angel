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


package com.tencent.angel.spark.ml.tree.gbdt.examples;

import com.tencent.angel.spark.ml.tree.data.Instance;
import com.tencent.angel.spark.ml.tree.gbdt.predictor.GBDTPredictor;
import com.tencent.angel.spark.ml.tree.util.DataLoader;
import java.util.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

public class GBDTPredictExample {

  public static void main(String[] argv) {
    SparkConf conf = new SparkConf().setMaster("local").setAppName("test");
    SparkContext sc = new SparkContext(conf);
    String modelPath = "xxx";
    GBDTPredictor predictor = new GBDTPredictor();
    predictor.loadModel(sc, modelPath);
    int dim = 181;
    // predict sparse instance with indices and values
    int[] indices = {6, 7, 11, 18, 20, 24, 27, 30, 33, 34, 38, 42, 45, 47, 53, 60, 61, 65, 69, 70, 75, 78, 79, 84, 87, 88, 92, 99, 101, 103, 108, 110, 112, 119, 123, 124, 128, 131, 134, 137, 139, 142, 147, 149, 156, 157, 161, 164, 166, 171, 173, 180};
    double[] values = new double[indices.length];
    Arrays.fill(values, 1);
    int result = predictor.predict(indices, values);
    System.out.println("Prediction with indices and values: " + result);
    float[] rawResult = predictor.predictRaw(indices, values);
    System.out.println("Prediction with indices and values: " + Arrays.toString(rawResult));
    // predict libsvm data
    String libsvmLine = "3 6:1 7:1 11:1 18:1 20:1 24:1 27:1 30:1 33:1 34:1 38:1 42:1 45:1 47:1 53:1 60:1 61:1 65:1 69:1 70:1 75:1 78:1 79:1 84:1 87:1 88:1 92:1 99:1 101:1 103:1 108:1 110:1 112:1 119:1 123:1 124:1 128:1 131:1 134:1 137:1 139:1 142:1 147:1 149:1 156:1 157:1 161:1 164:1 166:1 171:1 173:1 180:1";
    Instance instance = DataLoader.parseLibsvm(libsvmLine, dim);
    result = predictor.predict(instance.feature());
    System.out.println(String.format("Prediction with libsvm: %s, label %s", result, instance.label()));
    rawResult = predictor.predictRaw(instance.feature());
    System.out.println("Prediction with libsvm: " + Arrays.toString(rawResult));
  }

}

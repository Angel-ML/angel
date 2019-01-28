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


package com.tencent.angel.spark.ml.tree.gbdt.predictor;

import com.tencent.angel.spark.ml.tree.data.Instance;
import com.tencent.angel.spark.ml.tree.util.DataLoader;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

public class OnlinePredictExample {

  public static void main(String[] argv) {
    SparkConf conf = new SparkConf().setMaster("local").setAppName("test");
    SparkContext sc = new SparkContext(conf);
    String modelPath = "xxxx";
    GBDTPredictor predictor = new GBDTPredictor();
    predictor.loadModel(sc, modelPath);
    int dim = 47237;
    // predict sparse instance with indices and values
    int[] indices = {1, 100, 1000, 10000};
    double[] values = {1.0, 2.0, 3.0, 4.0};
    int result = predictor.predict(dim, indices, values);
    System.out.println("Prediction with indices and values: " + result);
    // predict libsvm data
    String libsvmLine = "-1 1523:0.107308708502892 2293:0.10189882251665 2621:0.234278331371989 2828:0.100789346447726 2964:0.118928729608936 4028:0.15810810172508 4392:0.251621393359619 5159:0.179302125575834 5573:0.0954362885656203 7632:0.275751998737705 8469:0.269591190597754 9706:0.240055355453941 11734:0.0905040206310084 12245:0.142613273060699 14003:0.234278331371989 14381:0.0694012500460769 15021:0.0712197969394981 16271:0.171999486718312 16809:0.0441606849743179 17278:0.141361974853942 18399:0.054062349671977 20445:0.148357397662057 21032:0.158102098039386 24437:0.0806807944402001 25713:0.0897947235410471 26924:0.1569843273045 29304:0.0496266005726542 31068:0.165700459649688 31513:0.122394577164188 31721:0.132255288315801 32277:0.0700633976580898 32439:0.0523174727572266 33711:0.250174245952023 36975:0.103258628626279 38775:0.107060201747063 39033:0.107012217629348 39397:0.0915237522010147 39463:0.0878878141686316 40238:0.0795205096564338 40783:0.14166898176731 42199:0.0689919035003698 44009:0.152281713855912 44439:0.123119867063594 46101:0.0752003525124999 46180:0.129177905766651 46871:0.234278331371989";
    Instance instance = DataLoader.parseLibsvm(libsvmLine, dim);
    result = predictor.predict(instance.feature());
    System.out
        .println(String.format("Prediction with libsvm: %s, label %s", result, instance.label()));
  }

}

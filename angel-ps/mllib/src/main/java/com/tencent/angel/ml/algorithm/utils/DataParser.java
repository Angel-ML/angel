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
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.angel.ml.algorithm.utils;

import com.tencent.angel.ml.feature.LabeledData;
import com.tencent.angel.ml.math.vector.SparseDoubleSortedVector;
import com.tencent.angel.ml.math.vector.SparseDummyVector;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class DataParser {

  public static LabeledData parse(LongWritable key, Text value, int maxDim, String actionType,
                                  String dataFormat) {
    switch (dataFormat) {
      case "dummy":
        return parseDummyVector(value, actionType, maxDim);
      case "libsvm":
        return parseLibsvmVector(value, actionType, maxDim);
    }

    return null;
  }

  public static LabeledData parseDummyVector(Text value, String actionType, int maxDim) {

    if (null == value) {
      return null;
    }
    String[] splits = value.toString().split(",");
    if (splits.length < 1) {
      return null;
    }
    SparseDummyVector x = new SparseDummyVector(maxDim, splits.length - 1);
    double y = Double.parseDouble(splits[0]);
    if (actionType.matches("train")) {
      y = y != 1 ? -1 : 1;
    }
    int i = 1;
    while (i < splits.length) {
      int index = Integer.valueOf(splits[i]);
      x.set(index, 1);
      i += 1;
    }
    return new LabeledData(x, y);
  }

  public static LabeledData parseLibsvmVector(Text value, String actionType, int maxDim) {
    if (null == value) {
      return null;
    }
    String[] splits = value.toString().trim().split(" ");
    if (splits.length < 1)
      return null;

    int len = splits.length - 1;
    int[] keys = new int[len];
    double[] vals = new double[len];
    double y = Double.parseDouble(splits[0]);

    // If y == 0, change y to -1, only when train
    if (actionType.matches("train"))
      y = y != 1 ? -1.0 : 1.0;

    for (int i = 0; i < len; i++) {
      String[] kv = splits[i + 1].split(":");
      int key = Integer.parseInt(kv[0]);
      double val = Double.parseDouble(kv[1]);
      keys[i] = key;
      vals[i] = val;
    }

    SparseDoubleSortedVector x = new SparseDoubleSortedVector(maxDim, keys, vals);
    return new LabeledData(x, y);
  }
}

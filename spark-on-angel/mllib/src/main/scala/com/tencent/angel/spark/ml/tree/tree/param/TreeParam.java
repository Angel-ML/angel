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

package com.tencent.angel.spark.ml.tree.tree.param;

import java.io.Serializable;

public abstract class TreeParam implements Serializable {

  public int numFeature;  // number of features
  public int maxDepth;  // maximum depth
  public int maxNodeNum;  // maximum node num
  public int numSplit;  // number of candidate splits
  public int numWorker;  // number of workers
  //public float insSampleRatio;  // subsample ratio for instances
  public float featSampleRatio;  // subsample ratio for features

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(String.format("|numFeature = %d\n", numFeature));
    sb.append(String.format("|maxDepth = %d\n", maxDepth));
    sb.append(String.format("|maxNodeNum = %d\n", maxNodeNum));
    sb.append(String.format("|numSplit = %d\n", numSplit));
    sb.append(String.format("|numWorker = %d\n", numWorker));
    sb.append(String.format("|featSampleRatio = %f\n", featSampleRatio));
    return sb.toString();
  }
}

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

package com.tencent.angel.spark.ml.psf.embedding;


/**
 * This class wraps some data structure that required to be maintained at server.
 */
public class ServerWrapper {

  // @maxIndex: this variable contains the max index of node/word
  private static volatile int maxIndex = -1;

  private static volatile int maxLength = -1;
  private static volatile int negative = -1;
  private static volatile int order = -1;
  private static volatile int partDim = -1;
  private static volatile int window = -1;

  private static int[] numInputs;
  private static int[] numOutputs;


  public static synchronized void initialize(int numPartitions, int maxIndex, int maxLength, int negative, int order,
                                             int partDim, int window) {

    numInputs = new int[numPartitions];
    numOutputs = new int[numPartitions];
    ServerWrapper.maxIndex = maxIndex;
    ServerWrapper.maxLength = maxLength;
    ServerWrapper.negative = negative;
    ServerWrapper.order = order;
    ServerWrapper.partDim = partDim;
    ServerWrapper.window = window;
  }

  public static int getMaxIndex() {
    return maxIndex;
  }

  public static int getMaxLength() {
    return maxLength;
  }

  public static int getNegative() {
    return negative;
  }

  public static int getOrder() {
    return order;
  }

  public static int getPartDim() {
    return partDim;
  }

  public static int getWindow() {
    return window;
  }

  public static void setNumInputs(int partitionId, int num) {
    numInputs[partitionId] = num;
  }

  public static int getNumInputs(int partitionId) {
    return numInputs[partitionId];
  }

  public static void setNumOutputs(int partitionId, int num) {
    numOutputs[partitionId] = num;
  }

  public static int getNumOutputs(int partitionId) {
    return numOutputs[partitionId];
  }

}

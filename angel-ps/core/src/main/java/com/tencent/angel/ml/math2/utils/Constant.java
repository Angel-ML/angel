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


package com.tencent.angel.ml.math2.utils;

public class Constant {

  public static double sparseThreshold = 3;
  public static double sparseDenseStorageThreshold = 0.35;
  public static double sortedDenseStorageThreshold = 0.50;
  public static double sparseSortedThreshold = 0.40;
  public static double sparseSortedStorageThreshold = 0.25;
  public static double sortedThreshold = 0.2;
  public static double intersectionCoeff = 0.75;
  public static double denseLoopThreshold = 0.3;
  public static double denseStorageThreshold = Math.pow(2, 10);
  public static Boolean keepStorage = false;
  public static Boolean isCompare = true;


  public static void setSparseThreshold(double sparseThreshold) {
    Constant.sparseThreshold = sparseThreshold;
  }

  public static void setSparseDenseStorageThreshold(double sparseDenseStorageThreshold) {
    Constant.sparseDenseStorageThreshold = sparseDenseStorageThreshold;
  }

  public static void setSortedDenseStorageThreshold(double sortedDenseStorageThreshold) {
    Constant.sortedDenseStorageThreshold = sortedDenseStorageThreshold;
  }

  public static void setSparseSortedThreshold(double sparseSortedThreshold) {
    Constant.sparseSortedThreshold = sparseSortedThreshold;
  }

  public static void setIntersectionCoeff(double intersectionCoeff) {
    Constant.intersectionCoeff = intersectionCoeff;
  }

  public static void setDenseLoopThreshold(double denseLoopThreshold) {
    Constant.denseLoopThreshold = denseLoopThreshold;
  }

  public static void setDenseStorageThreshold(double denseStorageThreshold) {
    Constant.denseStorageThreshold = denseStorageThreshold;
  }

  public static void setKeepStorage(Boolean keepStorage) {
    Constant.keepStorage = keepStorage;
  }

  public static void setIsCompare(Boolean isCompare) {
    Constant.isCompare = isCompare;
  }
}
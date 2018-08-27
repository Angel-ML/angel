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


package com.tencent.angel.model.output.format;

import com.tencent.angel.ps.ParameterServerId;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Model ouput utils
 */
public class ModelFilesUtils {
  private static final ConcurrentHashMap<Integer, AtomicInteger> psModelFileGens =
    new ConcurrentHashMap<>();

  /**
   * Get a new output file name for ps model, file name format : psId_index
   *
   * @param psId parameterserver id
   * @return a new file name
   */
  public static String nextFileName(ParameterServerId psId, int matrixId) {
    if (!psModelFileGens.containsKey(matrixId)) {
      psModelFileGens.putIfAbsent(matrixId, new AtomicInteger(0));
    }

    return psId + ModelFilesConstent.separator + psModelFileGens.get(matrixId).getAndIncrement();
  }

  /**
   * Get a output file name for ps model, file name format : psId_partid
   *
   * @param psId        parameterserver id
   * @param startPartId minimal partition id
   * @return
   */
  public static String fileName(ParameterServerId psId, int startPartId) {
    return psId + ModelFilesConstent.separator + startPartId;
  }
}

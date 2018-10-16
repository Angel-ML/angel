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

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Model ouput utils
 */
public class ModelFilesUtils {
  private static final ConcurrentHashMap<Integer, AtomicInteger> psModelFileGens =
    new ConcurrentHashMap<>();

  /**
   * Get a output file name for ps model, file name format : psId_partid
   *
   * @param startPartId minimal partition id
   * @return
   */
  public static String fileName(int startPartId) {
    return String.valueOf(startPartId);
  }

  public static MatrixFormat initFormat(String formatClass) throws IOException {
    try {
      return (MatrixFormat) Class.forName(formatClass).newInstance();
    } catch (Throwable e) {
      throw new IOException(e);
    }
  }
}

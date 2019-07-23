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

import com.tencent.angel.model.io.ColumnLoaderSaver;
import com.tencent.angel.model.io.MatrixLoaderSaver;
import com.tencent.angel.model.io.RowElementLoaderSaver;
import com.tencent.angel.model.io.RowLoaderSaver;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;

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
   */
  public static String fileName(int startPartId) {
    return String.valueOf(startPartId);
  }

  public static Format initFormat(String formatClass, Configuration conf) throws IOException {
    try {
      Constructor constructor = Class.forName(formatClass).getConstructor(Configuration.class);
      return (Format) constructor.newInstance(conf);
    } catch (Throwable e) {
      throw new IOException(e);
    }
  }

  public static MatrixLoaderSaver initLoaderSaver(Format format, Configuration conf) {
    if (format instanceof ElementFormat) {
      return new RowElementLoaderSaver((ElementFormat) format, conf);
    } else if (format instanceof ColumnFormat) {
      return new ColumnLoaderSaver((ColumnFormat) format, conf);
    } else if (format instanceof RowFormat) {
      return new RowLoaderSaver((RowFormat) format, conf);
    } else {
      throw new UnsupportedOperationException(
          "format " + format.getClass().getName() + " not support now!");
    }
  }
}

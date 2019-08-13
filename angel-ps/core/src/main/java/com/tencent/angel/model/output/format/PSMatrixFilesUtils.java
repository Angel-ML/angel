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

import com.tencent.angel.model.io.PSColumnLoaderSaver;
import com.tencent.angel.model.io.PSComplexRowLoaderSaver;
import com.tencent.angel.model.io.PSMatrixLoaderSaver;
import com.tencent.angel.model.io.PSRowElementLoaderSaver;
import com.tencent.angel.model.io.PSRowLoaderSaver;
import com.tencent.angel.model.io.SnapshotLoaderSaver;
import org.apache.hadoop.conf.Configuration;

public class PSMatrixFilesUtils {

  /**
   * Init loader/saver according to matrix format
   *
   * @param format matrix format
   * @param conf configuration
   * @return PS loader/saver
   */
  public static PSMatrixLoaderSaver initLoaderSaver(Format format, Configuration conf) {
    if (format instanceof ElementFormat) {
      return new PSRowElementLoaderSaver((ElementFormat) format, conf);
    } else if (format instanceof ColumnFormat) {
      return new PSColumnLoaderSaver((ColumnFormat) format, conf);
    } else if (format instanceof RowFormat) {
      return new PSRowLoaderSaver((RowFormat) format, conf);
    } else if (format instanceof ComplexRowFormat) {
      return new PSComplexRowLoaderSaver((ComplexRowFormat) format, conf);
    } else if(format instanceof SnapshotFormat) {
      return new SnapshotLoaderSaver((SnapshotFormat)format, conf);
    } else {
      throw new UnsupportedOperationException(
          "format " + format.getClass().getName() + " not support now!");
    }
  }
}

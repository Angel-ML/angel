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

package com.tencent.angel.conf;

public class MatrixConfiguration {

  public static final String SERVER_PARTITION_CLASS = "server.partition.class";
  public static final String DEFAULT_SERVER_PARTITION_CLASS =
      "com.tencent.ml.ps.impl.matrix.ServerPartition";
  
  public static final String MATRIX_OPLOG_TYPE = "matrix.oplog.type";
  public static final String DEFAULT_MATRIX_OPLOG_TYPE = "DENSE_DOUBE"; // LIL_INI, DENSE_DOUBLE, DENSE_INT
  
  public static final String MATRIX_OPLOG_ENABLEFILTER = "matrix.oplog.enablefilter"; // true, false
  public static final String DEFAULT_MATRIX_OPLOG_ENABLEFILTER = "true";

  public static final String MATRIX_STALENESS = "matrix.staleness";

  public static final String MATRIX_AVERAGE = "matrix.average";
  public static final String DEFAULT_MATRIX_AVERAGE = "false";
  
  public static final String MATRIX_HOGWILD = "matrix.hogwild";
  public static final String DEFAULT_MATRIX_HOGWILD = "false";

  public static final String MATRIX_PATH = "matrix.path";
  public static final String DEFAULT_MATRIX_PATH = "";

  public static final String MATRIX_LOAD_PATH = "matrix.load.path";
  public static final String DEFAULT_MATRIX_LOAD_PATH = "";

  public static final String MATRIX_SAVE_PATH = "matrix.save.path";
  public static final String DEFAULT_MATRIX_SAVE_PATH = "";

}

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


package com.tencent.angel.conf;

import com.tencent.angel.ps.storage.matrix.ServerPartition;

/**
 * Matrix parameters.
 */
public class MatrixConf {

  /**
   * matrix partitioner class
   */
  public static final String SERVER_PARTITION_CLASS = "server.partition.class";
  public static final String DEFAULT_SERVER_PARTITION_CLASS = ServerPartition.class.getName();

  /**
   * Matrix oplog storage type. There are four types oplog now:DENSE_DOUBLE, DENSE_INT, LIL_INI, DENSE_FLOAT.
   * <p>
   * DENSE_DOUBLE means use a dense double matrix to store the oplog, if the oplog is a sparse/dense
   * double vector or matrix, we can use it.
   * <p>
   * DENSE_FLOAT means use a dense float matrix to store the oplog, if the oplog is a sparse/dense
   * float vector or matrix, we can use it.
   * <p>
   * DENSE_INT means use a dense int matrix to store the oplog, if the oplog is a sparse/dense int
   * vector or matrix, we can use it.
   * <p>
   * LIL_INT means use a sparse int matrix to store the oplog, if the oplog is a sparse/dense int
   * vector or matrix, we can use it.
   */
  public static final String MATRIX_OPLOG_TYPE = "matrix.oplog.type";
  public static final String DEFAULT_MATRIX_OPLOG_TYPE = "DENSE_DOUBLE";

  /**
   * check and filter zero values in oplog before transfer
   */
  public static final String MATRIX_OPLOG_ENABLEFILTER = "matrix.oplog.enablefilter"; // true, false
  public static final String DEFAULT_MATRIX_OPLOG_ENABLEFILTER = "false";

  /**
   * check and filter zero values in oplog before transfer
   */
  public static final String MATRIX_OPLOG_FILTER_THRESHOLD = "matrix.oplog.filter.threshold";
    // true, false
  public static final String DEFAULT_MATRIX_OPLOG_FILTER_THRESHOLD = "0.0";

  /**
   * staleness value
   */
  public static final String MATRIX_STALENESS = "matrix.staleness";

  /**
   * Is we need divide oplog by task number. "true" means need, "false" means does not need.
   */
  public static final String MATRIX_AVERAGE = "matrix.average";
  public static final String DEFAULT_MATRIX_AVERAGE = "false";

  /**
   * enable hogwild mode. "true" means enable, "false" means disable
   */
  public static final String MATRIX_HOGWILD = "matrix.hogwild";
  public static final String DEFAULT_MATRIX_HOGWILD = "false";

  /**
   * existed matrix data files path
   */
  public static final String MATRIX_LOAD_PATH = "matrix.load.path";
  public static final String DEFAULT_MATRIX_LOAD_PATH = "";

  /**
   * matrix data files save path
   */
  public static final String MATRIX_SAVE_PATH = "matrix.save.path";
  public static final String DEFAULT_MATRIX_SAVE_PATH = "";
}

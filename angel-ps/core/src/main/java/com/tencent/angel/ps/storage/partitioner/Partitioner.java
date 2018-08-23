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


package com.tencent.angel.ps.storage.partitioner;

import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.ml.matrix.PartitionMeta;
import org.apache.hadoop.conf.Configuration;

import java.util.List;

/**
 * Matrix partitioner interface.
 */
public interface Partitioner {
  /**
   * Init matrix partitioner
   *
   * @param mContext matrix context
   * @param conf
   */
  void init(MatrixContext mContext, Configuration conf);

  /**
   * Generate the partitions for the matrix
   *
   * @return the partitions for the matrix
   */
  List<PartitionMeta> getPartitions();

  /**
   * Assign a matrix partition to a parameter server
   *
   * @param partId matrix partition id
   * @return parameter server index
   */
  int assignPartToServer(int partId);
}

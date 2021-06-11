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
package com.tencent.angel.graph.common.data;

import com.tencent.angel.ml.math2.matrix.Matrix;
import com.tencent.angel.model.MatrixLoadContext;
import com.tencent.angel.model.PSMatrixLoadContext;
import com.tencent.angel.model.PSMatrixSaveContext;
import com.tencent.angel.model.output.format.MatrixPartitionMeta;
import com.tencent.angel.model.output.format.RowBasedFormat;
import com.tencent.angel.ps.storage.partition.RowBasedPartition;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;

public class NodeBinaryFormat extends RowBasedFormat {

  public NodeBinaryFormat(Configuration conf) {
    super(conf);
  }

  @Override
  public void load(RowBasedPartition part, MatrixPartitionMeta partMeta,
      PSMatrixLoadContext loadContext, DataInputStream input) throws IOException {

  }

  @Override
  public void save(RowBasedPartition part, MatrixPartitionMeta partMeta,
      PSMatrixSaveContext saveContext, DataOutputStream output) throws IOException {

  }

  @Override
  public void load(Matrix matrix, MatrixPartitionMeta partMeta, MatrixLoadContext loadContext,
      FSDataInputStream in) throws IOException {

  }
}

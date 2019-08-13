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


package com.tencent.angel.master;

import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.ml.math2.utils.RowType;
import com.tencent.angel.ps.storage.partitioner.ColumnRangePartitioner;
import com.tencent.angel.ps.storage.partitioner.RangePartitioner;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class PartitionerTest {
  @Test public void testModelLongPartitioner() throws Exception {
    MatrixContext mMatrix = new MatrixContext();
    mMatrix.setName("w1");
    mMatrix.setRowNum(10);
    mMatrix.setColNum(-1);
    mMatrix.setValidIndexNum(100000000);
    mMatrix.setRowType(RowType.T_DOUBLE_SPARSE_LONGKEY);
    Configuration conf = new Configuration();
    conf.setInt(AngelConf.ANGEL_PS_NUMBER, 1);
    ColumnRangePartitioner partitioner = new ColumnRangePartitioner();
    partitioner.init(mMatrix, conf);
    partitioner.getPartitions();
  }
}

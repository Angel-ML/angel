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

package com.tencent.angel.ps;

import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.protobuf.generated.MLProtos;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.log4j.PropertyConfigurator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class PSPartitionerTest {
  private final static Log LOG = LogFactory.getLog(PSPartitionerTest.class);
  protected MatrixContext mContext;
  private Configuration conf = new YarnConfiguration();
  private PSPartitioner partitioner;
  private int matrixId;

  static {
    PropertyConfigurator.configure("../conf/log4j.properties");
  }

  @Before
  public void setUp() throws Exception {
    mContext = new MatrixContext("m", 10, 10, 6, 6);
    partitioner = new PSPartitioner();
    partitioner.init(mContext, conf);
    matrixId = mContext.getId();
  }

  @After
  public void tearDown() throws Exception {}

  @Test
  public void testSetup() throws Exception {}

  @Test
  public void testGetServerIndex() throws Exception {}

  @Test
  public void testGenerateMatrixProto() throws Exception {}

  @Test
  public void testGetPartitions() throws Exception {
    List<MLProtos.Partition> array = new ArrayList<MLProtos.Partition>();
    MLProtos.Partition.Builder partition_1 = MLProtos.Partition.newBuilder();
    MLProtos.Partition.Builder partition_2 = MLProtos.Partition.newBuilder();
    MLProtos.Partition.Builder partition_3 = MLProtos.Partition.newBuilder();
    MLProtos.Partition.Builder partition_4 = MLProtos.Partition.newBuilder();
    // Note:[startRow,endRow)
    partition_1.setMatrixId(matrixId);
    partition_1.setPartitionId(0);
    partition_1.setStartRow(0);
    partition_1.setStartCol(0);
    partition_1.setEndRow(6);
    partition_1.setEndCol(6);
    array.add(partition_1.build());

    partition_2.setMatrixId(matrixId);
    partition_2.setPartitionId(1);
    partition_2.setStartRow(0);
    partition_2.setStartCol(6);
    partition_2.setEndRow(6);
    partition_2.setEndCol(10);
    array.add(partition_2.build());

    partition_3.setMatrixId(matrixId);
    partition_3.setPartitionId(2);
    partition_3.setStartRow(6);
    partition_3.setStartCol(0);
    partition_3.setEndRow(10);
    partition_3.setEndCol(6);
    array.add(partition_3.build());

    partition_4.setMatrixId(matrixId);
    partition_4.setPartitionId(3);
    partition_4.setStartRow(6);
    partition_4.setStartCol(6);
    partition_4.setEndRow(10);
    partition_4.setEndCol(10);
    array.add(partition_4.build());
    assertEquals(array, partitioner.getPartitions());
  }

  @Test
  public void testGetMatrixId() throws Exception {}

  @Test
  public void testGenerateMatrixId() throws Exception {}

  @Test
  public void testGetMatrixContext() throws Exception {}
}

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

package com.tencent.angel.ps.impl.matrix;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.protobuf.generated.MLProtos;
import com.tencent.angel.protobuf.generated.PSMasterServiceProtos.MatrixPartition;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.PropertyConfigurator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;

import static org.junit.Assert.*;

public class ServerMatrixTest {
  private static final Log LOG = LogFactory.getLog(ServerMatrixTest.class);
  private MatrixPartition matrix;
  private ServerMatrix serverMatrix;

  static {
    PropertyConfigurator.configure("../conf/log4j.properties");
  }

  @Before
  public void setup() throws Exception {
    MatrixPartition.Builder mpBuilder = MatrixPartition.newBuilder();
    mpBuilder.setMatrixId(1);
    mpBuilder.setMatrixName("test");
    mpBuilder.setRowNum(2);
    mpBuilder.setColNum(1000);
    mpBuilder.setRowType(MLProtos.RowType.T_INT_DENSE);
    MLProtos.Partition.Builder partitionBuilder = MLProtos.Partition.newBuilder();
    partitionBuilder.setStartRow(0);
    partitionBuilder.setEndRow(1);
    partitionBuilder.setStartCol(1);
    partitionBuilder.setEndCol(501);
    partitionBuilder.setMatrixId(1);
    partitionBuilder.setPartitionId(1);
    mpBuilder.addPartitions(partitionBuilder.build());

    partitionBuilder.setStartRow(1);
    partitionBuilder.setEndRow(2);
    partitionBuilder.setStartCol(7);
    partitionBuilder.setEndCol(9);
    partitionBuilder.setMatrixId(1);
    partitionBuilder.setPartitionId(2);
    mpBuilder.addPartitions(partitionBuilder.build());
    matrix = mpBuilder.build();
    serverMatrix = new ServerMatrix(matrix);
  }

  @After
  public void tearDown() throws Exception {
    File file = new File("data");
    if (file.isFile() && file.exists())
      file.delete();
  }

  @Test
  public void testGetPartition() throws Exception {
    assertNotNull(serverMatrix.getPartition(1));
  }

  @Test
  public void testGetTotalPartitionKeys() throws Exception {
    assertEquals(serverMatrix.getTotalPartitionKeys().size(), 2);
  }

  @Test
  public void testGetName() throws Exception {
    assertEquals(serverMatrix.getName(), "test");
  }

  @Test
  public void testGetId() throws Exception {
    assertEquals(serverMatrix.getId(), 1);
  }

  @Test
  public void testReadFrom() throws Exception {
    MatrixPartition.Builder mpBuilderNew = MatrixPartition.newBuilder();
    mpBuilderNew.setMatrixId(2);
    mpBuilderNew.setMatrixName("new");
    mpBuilderNew.setRowNum(1);
    mpBuilderNew.setColNum(1000);
    mpBuilderNew.setRowType(MLProtos.RowType.T_INT_DENSE);
    MLProtos.Partition.Builder partitionBuilderNew = MLProtos.Partition.newBuilder();
    partitionBuilderNew.setStartRow(0);
    partitionBuilderNew.setEndRow(1);
    partitionBuilderNew.setStartCol(1);
    partitionBuilderNew.setEndCol(605);
    partitionBuilderNew.setMatrixId(2);
    partitionBuilderNew.setPartitionId(1);
    mpBuilderNew.addPartitions(partitionBuilderNew.build());
    MatrixPartition matrixNew = mpBuilderNew.build();
    ServerMatrix ServerMatrixNew = new ServerMatrix(matrixNew);
    DataOutputStream out = new DataOutputStream(new FileOutputStream("data"));
    ServerMatrixNew.writeTo(out);
    out.close();
    assertEquals(1, ServerMatrixNew.getTotalPartitionKeys().size());
    DataInputStream in = new DataInputStream(new FileInputStream("data"));
    serverMatrix.readFrom(in);
  }

  @Test
  public void testWriteTo() throws Exception {
    DataOutputStream out = new DataOutputStream(new FileOutputStream("data"));
    serverMatrix.writeTo(out);
    out.close();
    DataInputStream in = new DataInputStream(new FileInputStream("data"));
    assertEquals(2, in.readInt());// size
    in.close();
  }

  @Test
  public void testWriteHeader() throws Exception {
    DataOutputStream out = new DataOutputStream(new FileOutputStream("data"));
    serverMatrix.writeHeader(out);
    out.close();
    DataInputStream in = new DataInputStream(new FileInputStream("data"));
    assertEquals(1, in.readInt());
    assertEquals(2, in.readInt());// size
    in.close();
  }

  @Test
  public void testReadHeader() throws Exception {}

  @Test
  public void testGetPartition1() throws Exception {}

  @Test
  public void testGetClocks() throws Exception {
    Object2IntOpenHashMap<PartitionKey> clocks = new Object2IntOpenHashMap<PartitionKey>();
    assertTrue(clocks.isEmpty());
    serverMatrix.getClocks(clocks);
    assertEquals(clocks.size(), 2);
  }

  @Test
  public void testSetClock() throws Exception {}
}

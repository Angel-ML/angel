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
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.tencent.angel.ps.impl.matrix;

import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.protobuf.generated.MLProtos;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.PropertyConfigurator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;

import static org.junit.Assert.assertEquals;

public class ServerSparseDoubleRowTest {
  private final static Log LOG = LogFactory.getLog(ServerSparseDoubleRowTest.class);
  private ServerSparseDoubleRow serverSparseDoubleRow;
  private int rowId;
  private int startCol;
  private int endCol;

  static {
    PropertyConfigurator.configure("../conf/log4j.properties");
  }

  @Before
  public void setUp() throws Exception {
    rowId = 0;
    startCol = 0;
    endCol = 3;
    serverSparseDoubleRow = new ServerSparseDoubleRow(rowId, startCol, endCol, 0);
  }

  @After
  public void tearDown() throws Exception {
    File file = new File("data");
    if (file.isFile() && file.exists())
      file.delete();
  }

  @Test
  public void testWriteTo() throws Exception {
    ByteBuf buf = Unpooled.buffer(4 + 8 * 3);
    buf.writeInt(3);
    buf.writeDouble(0.00);
    buf.writeDouble(1.00);
    buf.writeDouble(2.00);
    serverSparseDoubleRow.update(RowType.T_DOUBLE_DENSE, buf);
    DataOutputStream out = new DataOutputStream(new FileOutputStream("data"));
    serverSparseDoubleRow.writeTo(out);
    out.close();
    DataInputStream in = new DataInputStream(new FileInputStream("data"));
    assertEquals(in.readInt(), 3);
    Int2DoubleOpenHashMap hashMap = new Int2DoubleOpenHashMap();
    hashMap.addTo(0, 0.00);
    hashMap.addTo(1, 1.00);
    hashMap.addTo(2, 2.00);
    assertEquals(serverSparseDoubleRow.getData(), hashMap);
  }

  @Test
  public void testReadFrom() throws Exception {
    ByteBuf buf = Unpooled.buffer(4 + 8 * 3);
    buf.writeInt(3);
    buf.writeDouble(10.00);
    buf.writeDouble(11.00);
    buf.writeDouble(12.00);
    serverSparseDoubleRow.update(RowType.T_DOUBLE_DENSE, buf);
    DataOutputStream out = new DataOutputStream(new FileOutputStream("data"));
    serverSparseDoubleRow.writeTo(out);
    out.close();
    DataInputStream in = new DataInputStream(new FileInputStream("data"));
    ServerSparseDoubleRow newServerSparseDoubleRow =
        new ServerSparseDoubleRow(rowId, startCol, endCol, 0);
    newServerSparseDoubleRow.readFrom(in);
    in.close();

    assertEquals(newServerSparseDoubleRow.getData().get(0), serverSparseDoubleRow.getData().get(0),
        0.00);
    assertEquals(newServerSparseDoubleRow.getData().get(1), serverSparseDoubleRow.getData().get(1),
        0.00);
    assertEquals(newServerSparseDoubleRow.getData().get(2), serverSparseDoubleRow.getData().get(2),
        0.00);

  }

  @Test
  public void testGetRowType() throws Exception {
    assertEquals(serverSparseDoubleRow.getRowType(), RowType.T_DOUBLE_SPARSE);
  }

  @Test
  public void testUpdate() throws Exception {
    serverSparseDoubleRow = new ServerSparseDoubleRow(rowId, startCol, endCol, 0);
    ByteBuf buf = Unpooled.buffer(4 + 8 * 3);
    buf.writeInt(3);
    buf.writeDouble(0.00);
    buf.writeDouble(1.00);
    buf.writeDouble(-1.00);
    double newValue0 = buf.getDouble(4) + serverSparseDoubleRow.getData().get(0);
    double newValue1 = buf.getDouble(12) + serverSparseDoubleRow.getData().get(1);
    serverSparseDoubleRow.update(RowType.T_DOUBLE_DENSE, buf);
    assertEquals(serverSparseDoubleRow.getData().get(0), newValue0, 0.000);
    assertEquals(serverSparseDoubleRow.getData().get(1), newValue1, 0.000);
    assertEquals(serverSparseDoubleRow.getData().get(2), -1, 0.000);

    serverSparseDoubleRow = new ServerSparseDoubleRow(rowId, startCol, endCol, 0);
    buf = Unpooled.buffer(4 + 2 * 12);
    buf.writeInt(2);
    LOG.info(buf);
    buf.writeInt(0);
    buf.writeDouble(1.00);
    buf.writeInt(2);
    buf.writeDouble(-2.00);
    serverSparseDoubleRow.update(RowType.T_DOUBLE_SPARSE, buf);
    assertEquals(serverSparseDoubleRow.getData().get(0), 1, 0.000);
    assertEquals(serverSparseDoubleRow.getData().get(1), 0, 0.000);
    assertEquals(serverSparseDoubleRow.getData().get(2), -2, 0.000);
  }

  @Test
  public void testSerialize() throws Exception {
    ByteBuf buf = Unpooled.buffer(4 + 8 * 3);
    buf.writeInt(3);
    serverSparseDoubleRow.setClock(8);
    buf.writeDouble(0.00);
    buf.writeDouble(1.00);
    buf.writeDouble(-1.00);
    serverSparseDoubleRow.update(RowType.T_DOUBLE_DENSE, buf);
    serverSparseDoubleRow.serialize(buf);
    assertEquals(serverSparseDoubleRow.getRowId(), buf.readInt());
    assertEquals(serverSparseDoubleRow.getClock(), buf.readInt());
    assertEquals(serverSparseDoubleRow.getStartCol(), buf.readLong());
    assertEquals(serverSparseDoubleRow.getEndCol(), buf.readLong());
    assertEquals(serverSparseDoubleRow.getRowVersion(), buf.readInt());
    assertEquals(3, buf.readInt());
  }

  @Test
  public void testDeserialize() throws Exception {
    ByteBuf buf = Unpooled.buffer(16);
    buf.writeInt(0);
    buf.writeInt(1);
    buf.writeLong(2);
    buf.writeLong(3);
    buf.writeInt(4);
    buf.writeInt(3);
    buf.writeInt(0);
    buf.writeDouble(-1.0);
    buf.writeInt(1);
    buf.writeDouble(-2.0);
    buf.writeInt(2);
    buf.writeDouble(-3.0);
    serverSparseDoubleRow.deserialize(buf);
    assertEquals(serverSparseDoubleRow.getRowId(), 0);
    assertEquals(serverSparseDoubleRow.getClock(), 1);
    assertEquals(serverSparseDoubleRow.getStartCol(), 2);
    assertEquals(serverSparseDoubleRow.getEndCol(), 3);
    assertEquals(serverSparseDoubleRow.getRowVersion(), 4);
    assertEquals(serverSparseDoubleRow.getData().size(), 3);
    assertEquals(serverSparseDoubleRow.getData().get(0), -1, 0.0);
    assertEquals(serverSparseDoubleRow.getData().get(1), -2, 0.0);
    assertEquals(serverSparseDoubleRow.getData().get(2), -3, 0.0);
  }

  @Test
  public void testBufferLen() throws Exception {
    ByteBuf buf = Unpooled.buffer(4 + 8 * 3);
    buf.writeInt(3);
    serverSparseDoubleRow.setClock(8);
    buf.writeDouble(0.00);
    buf.writeDouble(1.00);
    buf.writeDouble(-1.00);
    serverSparseDoubleRow.update(RowType.T_DOUBLE_DENSE, buf);
    assertEquals(serverSparseDoubleRow.bufferLen(), 28 + 4 + 3 * 12);
  }

  @Test
  public void testMergeDoubleDense() throws Exception {

  }

  @Test
  public void testMergeDoubleSparse() throws Exception {

  }

  @Test
  public void testMergeTo() throws Exception {
    ByteBuf buf = Unpooled.buffer(4 + 8 * 3);
    buf.writeInt(3);
    serverSparseDoubleRow.setClock(8);
    buf.writeDouble(0.00);
    buf.writeDouble(1.00);
    buf.writeDouble(-1.00);
    serverSparseDoubleRow.update(RowType.T_DOUBLE_DENSE, buf);
    int[] index = new int[3];
    double[] value = new double[3];
    serverSparseDoubleRow.mergeTo(index, value, 0, 3);
    // LOG.info(index[0] + " " + value[0]);
    // LOG.info(index[1] + " " + value[1]);
    // LOG.info(index[2] + " " + value[2]);
  }

  @Test
  public void testMergeTo1() throws Exception {

  }
}

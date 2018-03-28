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

import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.protobuf.generated.MLProtos;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.PropertyConfigurator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;

import static org.junit.Assert.assertEquals;


public class ServerDenseIntRowTest {
  private final static Log LOG = LogFactory.getLog(ServerDenseIntRowTest.class);
  private ServerDenseIntRow serverDenseIntRow;
  private int rowId;
  private int startCol;
  private int endCol;
  private byte[] buffer;

  static {
    PropertyConfigurator.configure("../conf/log4j.properties");
  }

  @Before
  public void setUp() throws Exception {
    rowId = 0;
    startCol = 0;
    endCol = 3;
    buffer = new byte[24];
    serverDenseIntRow = new ServerDenseIntRow(rowId, startCol, endCol);
  }

  @After
  public void tearDown() throws Exception {
    File file = new File("data");
    if (file.isFile() && file.exists())
      file.delete();
  }

  @Test
  public void testGetRowType() throws Exception {
    assertEquals(serverDenseIntRow.getRowType(), RowType.T_INT_DENSE);
  }

  @Test
  public void testSize() throws Exception {
    assertEquals(serverDenseIntRow.size(), endCol - startCol);
  }

  @Test
  public void testUpdate() throws Exception {
    serverDenseIntRow = new ServerDenseIntRow(rowId, startCol, endCol);
    ByteBuf buf = Unpooled.buffer(16);
    buf.writeInt(3);
    buf.writeInt(0);
    buf.writeInt(1);
    buf.writeInt(-1);
    int newValue0 = buf.getInt(4) + serverDenseIntRow.getData().get(0);
    int newValue1 = buf.getInt(8) + serverDenseIntRow.getData().get(1);
    serverDenseIntRow.update(RowType.T_INT_DENSE, buf);
    assertEquals(serverDenseIntRow.getData().get(0), newValue0, 0.000);
    assertEquals(serverDenseIntRow.getData().get(1), newValue1, 0.000);
    assertEquals(serverDenseIntRow.getData().get(2), -1, 0.000);

    serverDenseIntRow = new ServerDenseIntRow(rowId, startCol, endCol);
    buf = Unpooled.buffer(4 + 8 * 2);
    buf.writeInt(2);
    buf.writeInt(0);
    buf.writeInt(1);
    buf.writeInt(2);
    buf.writeInt(-2);
    serverDenseIntRow.update(RowType.T_INT_SPARSE, buf);
    assertEquals(serverDenseIntRow.getData().get(0), 1, 0.000);
    assertEquals(serverDenseIntRow.getData().get(1), 0, 0.000);
    assertEquals(serverDenseIntRow.getData().get(2), -2, 0.000);
  }


  @Test
  public void testWriteTo() throws Exception {
    ByteBuf buf = Unpooled.buffer(16);
    buf.writeInt(3);
    buf.writeInt(0);
    buf.writeInt(1);
    buf.writeInt(2);
    serverDenseIntRow.update(RowType.T_INT_DENSE, buf);

    DataOutputStream out = new DataOutputStream(new FileOutputStream("data"));
    serverDenseIntRow.writeTo(out);
    out.close();
    DataInputStream in = new DataInputStream(new FileInputStream("data"));
    assertEquals(in.readInt(), 0, 0.00);
    assertEquals(in.readInt(), 1, 0.00);
    assertEquals(in.readInt(), 2, 0.00);
    in.close();
  }

  @Test
  public void testReadFrom() throws Exception {
    ByteBuf buf = Unpooled.buffer(16);
    buf.writeInt(3);
    buf.writeInt(10);
    buf.writeInt(11);
    buf.writeInt(12);
    serverDenseIntRow.update(RowType.T_INT_DENSE, buf);

    DataOutputStream out = new DataOutputStream(new FileOutputStream("data"));
    serverDenseIntRow.writeTo(out);
    out.close();
    DataInputStream in = new DataInputStream(new FileInputStream("data"));
    ServerDenseIntRow newServerDenseIntRow = new ServerDenseIntRow(rowId, startCol, endCol);
    newServerDenseIntRow.readFrom(in);
    in.close();

    assertEquals(newServerDenseIntRow.getData().get(0), serverDenseIntRow.getData().get(0), 0.00);
    assertEquals(newServerDenseIntRow.getData().get(1), serverDenseIntRow.getData().get(1), 0.00);
    assertEquals(newServerDenseIntRow.getData().get(2), serverDenseIntRow.getData().get(2), 0.00);
  }

  public ServerDenseIntRowTest() {
    super();
  }

  @Test
  public void testSerialize() throws Exception {
    ByteBuf buf = Unpooled.buffer(16);
    serverDenseIntRow.setClock(8);
    serverDenseIntRow.serialize(buf);
    assertEquals(serverDenseIntRow.getRowId(), buf.readInt());
    assertEquals(serverDenseIntRow.getClock(), buf.readInt());
    assertEquals(serverDenseIntRow.getStartCol(), buf.readLong());
    assertEquals(serverDenseIntRow.getEndCol(), buf.readLong());
    assertEquals(serverDenseIntRow.getRowVersion(), buf.readInt());
    assertEquals(serverDenseIntRow.getEndCol() - serverDenseIntRow.getStartCol(), buf.readInt());
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
    buf.writeInt(-1);
    buf.writeInt(-2);
    buf.writeInt(-3);
    serverDenseIntRow.deserialize(buf);
    assertEquals(serverDenseIntRow.getRowId(), 0);
    assertEquals(serverDenseIntRow.getClock(), 1);
    assertEquals(serverDenseIntRow.getStartCol(), 2);
    assertEquals(serverDenseIntRow.getEndCol(), 3);
    assertEquals(serverDenseIntRow.getRowVersion(), 4);
    assertEquals(serverDenseIntRow.getData().get(0), -1, 0.0);
    assertEquals(serverDenseIntRow.getData().get(1), -2, 0.0);
    assertEquals(serverDenseIntRow.getData().get(2), -3, 0.0);
  }

  @Test
  public void testMergeTo() throws Exception {
    ByteBuf buf = Unpooled.buffer(16);
    buf.writeInt(3);
    buf.writeInt(10);
    buf.writeInt(11);
    buf.writeInt(12);
    serverDenseIntRow.update(RowType.T_INT_DENSE, buf);
    int[] dataArray = {0, 1, 2, 3, 4};
    serverDenseIntRow.mergeTo(dataArray);
    assertEquals(dataArray[0], 10, 0.00);
    assertEquals(dataArray[1], 11, 0.00);
    assertEquals(dataArray[2], 12, 0.00);
    assertEquals(dataArray[3], 3, 0.00);
    assertEquals(dataArray[4], 4, 0.00);
  }
}

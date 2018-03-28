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


public class ServerDenseDoubleRowTest {
  private final static Log LOG = LogFactory.getLog(ServerDenseDoubleRowTest.class);
  private ServerDenseDoubleRow serverDenseDoubleRow;
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
    serverDenseDoubleRow = new ServerDenseDoubleRow(rowId, startCol, endCol);
  }

  @After
  public void tearDown() throws Exception {
    File file = new File("data");
    if (file.isFile() && file.exists())
      file.delete();
  }

  @Test
  public void testGetRowType() throws Exception {
    assertEquals(serverDenseDoubleRow.getRowType(), RowType.T_DOUBLE_DENSE);
  }

  @Test
  public void testSize() throws Exception {
    assertEquals(serverDenseDoubleRow.size(), endCol - startCol);
  }

  @Test
  public void testUpdate() throws Exception {
    serverDenseDoubleRow = new ServerDenseDoubleRow(rowId, startCol, endCol);
    ByteBuf buf = Unpooled.buffer(24);
    buf.writeInt(3);
    buf.writeDouble(0.00);
    buf.writeDouble(1.00);
    buf.writeDouble(-1.00);

    double newValue0 = buf.getDouble(4) + serverDenseDoubleRow.getData().get(0);
    double newValue1 = buf.getDouble(12) + serverDenseDoubleRow.getData().get(1);
    serverDenseDoubleRow.update(RowType.T_DOUBLE_DENSE, buf);
    assertEquals(serverDenseDoubleRow.getData().get(0), newValue0, 0.000);
    assertEquals(serverDenseDoubleRow.getData().get(1), newValue1, 0.000);
    assertEquals(serverDenseDoubleRow.getData().get(2), -1, 0.000);

    serverDenseDoubleRow = new ServerDenseDoubleRow(rowId, startCol, endCol);
    buf = Unpooled.buffer(4 + 12 * 2);
    LOG.info(buf);
    buf.writeInt(2);
    buf.writeInt(0);
    buf.writeDouble(1.00);
    buf.writeInt(2);
    buf.writeDouble(-2.00);
    serverDenseDoubleRow.update(RowType.T_DOUBLE_SPARSE, buf);
    assertEquals(serverDenseDoubleRow.getData().get(0), 1, 0.000);
    assertEquals(serverDenseDoubleRow.getData().get(1), 0, 0.000);
    assertEquals(serverDenseDoubleRow.getData().get(2), -2, 0.000);

  }


  @Test
  public void testWriteTo() throws Exception {
    ByteBuf buf = Unpooled.buffer(28);
    buf.writeInt(3);
    buf.writeDouble(0.00);
    buf.writeDouble(1.00);
    buf.writeDouble(2.00);
    serverDenseDoubleRow.update(RowType.T_DOUBLE_DENSE, buf);
    DataOutputStream out = new DataOutputStream(new FileOutputStream("data"));
    serverDenseDoubleRow.writeTo(out);
    out.close();
    DataInputStream in = new DataInputStream(new FileInputStream("data"));
    assertEquals(in.readDouble(), 0, 0.00);
    assertEquals(in.readDouble(), 1, 0.00);
    assertEquals(in.readDouble(), 2, 0.00);
    in.close();
  }

  @Test
  public void testReadFrom() throws Exception {
    ByteBuf buf = Unpooled.buffer(28);
    buf.writeInt(3);
    buf.writeDouble(10.00);
    buf.writeDouble(11.00);
    buf.writeDouble(12.00);
    serverDenseDoubleRow.update(RowType.T_DOUBLE_DENSE, buf);
    DataOutputStream out = new DataOutputStream(new FileOutputStream("data"));
    serverDenseDoubleRow.writeTo(out);
    out.close();
    DataInputStream in = new DataInputStream(new FileInputStream("data"));
    ServerDenseDoubleRow newServerDenseDoubleRow =
        new ServerDenseDoubleRow(rowId, startCol, endCol);
    newServerDenseDoubleRow.readFrom(in);
    in.close();
    assertEquals(newServerDenseDoubleRow.getData().get(0), serverDenseDoubleRow.getData().get(0),
        0.00);
    assertEquals(newServerDenseDoubleRow.getData().get(1), serverDenseDoubleRow.getData().get(1),
        0.00);
    assertEquals(newServerDenseDoubleRow.getData().get(2), serverDenseDoubleRow.getData().get(2),
        0.00);
  }

  public ServerDenseDoubleRowTest() {
    super();
  }

  @Test
  public void testSerialize() throws Exception {
    ByteBuf buf = Unpooled.buffer(16);
    serverDenseDoubleRow.setClock(8);
    serverDenseDoubleRow.serialize(buf);
    assertEquals(serverDenseDoubleRow.getRowId(), buf.readInt());
    assertEquals(serverDenseDoubleRow.getClock(), buf.readInt());
    assertEquals(serverDenseDoubleRow.getStartCol(), buf.readLong());
    assertEquals(serverDenseDoubleRow.getEndCol(), buf.readLong());
    assertEquals(serverDenseDoubleRow.getRowVersion(), buf.readInt());
    assertEquals(serverDenseDoubleRow.getEndCol() - serverDenseDoubleRow.getStartCol(),
        buf.readInt());
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
    buf.writeDouble(-1.0);
    buf.writeDouble(-2.0);
    buf.writeDouble(-3.0);
    serverDenseDoubleRow.deserialize(buf);
    assertEquals(serverDenseDoubleRow.getRowId(), 0);
    assertEquals(serverDenseDoubleRow.getClock(), 1);
    assertEquals(serverDenseDoubleRow.getStartCol(), 2);
    assertEquals(serverDenseDoubleRow.getEndCol(), 3);
    assertEquals(serverDenseDoubleRow.getRowVersion(), 4);
    assertEquals(serverDenseDoubleRow.getData().get(0), -1, 0.0);
    assertEquals(serverDenseDoubleRow.getData().get(1), -2, 0.0);
    assertEquals(serverDenseDoubleRow.getData().get(2), -3, 0.0);
  }

  @Test
  public void testMergeTo() throws Exception {
    ByteBuf buf = Unpooled.buffer(28);
    buf.writeInt(3);
    buf.writeDouble(10.00);
    buf.writeDouble(11.00);
    buf.writeDouble(12.00);
    serverDenseDoubleRow.update(RowType.T_DOUBLE_DENSE, buf);
    double[] dataArray = {0, 1, 2, 3, 4};
    serverDenseDoubleRow.mergeTo(dataArray);
    assertEquals(dataArray[0], 10, 0.00);
    assertEquals(dataArray[1], 11, 0.00);
    assertEquals(dataArray[2], 12, 0.00);
    assertEquals(dataArray[3], 3, 0.00);
    assertEquals(dataArray[4], 4, 0.00);
  }
}

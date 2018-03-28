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


public class ServerDenseFloatRowTest {
  private final static Log LOG = LogFactory.getLog(ServerDenseFloatRowTest.class);
  private ServerDenseFloatRow serverDenseFloatRow;
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
    serverDenseFloatRow = new ServerDenseFloatRow(rowId, startCol, endCol);
  }

  @After
  public void tearDown() throws Exception {
    File file = new File("data");
    if (file.isFile() && file.exists())
      file.delete();
  }

  @Test
  public void testGetRowType() throws Exception {
    assertEquals(serverDenseFloatRow.getRowType(), RowType.T_FLOAT_DENSE);
  }

  @Test
  public void testSize() throws Exception {
    assertEquals(serverDenseFloatRow.size(), endCol - startCol);
  }

  @Test
  public void testUpdate() throws Exception {
    serverDenseFloatRow = new ServerDenseFloatRow(rowId, startCol, endCol);
    ByteBuf buf = Unpooled.buffer(16);
    buf.writeInt(3);
    buf.writeFloat((float) 0.00);
    buf.writeFloat((float) 1.00);
    buf.writeFloat((float) -1.00);
    float newValue0 = buf.getFloat(4) + serverDenseFloatRow.getData().get(0);
    float newValue1 = buf.getFloat(8) + serverDenseFloatRow.getData().get(1);
    serverDenseFloatRow.update(RowType.T_FLOAT_DENSE, buf);
    assertEquals(serverDenseFloatRow.getData().get(0), newValue0, 0.000);
    assertEquals(serverDenseFloatRow.getData().get(1), newValue1, 0.000);
    assertEquals(serverDenseFloatRow.getData().get(2), -1, 0.000);

    serverDenseFloatRow = new ServerDenseFloatRow(rowId, startCol, endCol);
    buf = Unpooled.buffer(4 + 8 * 2);
    buf.writeInt(2);
    buf.writeInt(0);
    buf.writeFloat((float) 1.00);
    buf.writeInt(2);
    buf.writeFloat((float) -2.00);
    serverDenseFloatRow.update(RowType.T_FLOAT_SPARSE, buf);
    assertEquals(serverDenseFloatRow.getData().get(0), 1, 0.000);
    assertEquals(serverDenseFloatRow.getData().get(1), 0, 0.000);
    assertEquals(serverDenseFloatRow.getData().get(2), -2, 0.000);
  }


  @Test
  public void testWriteTo() throws Exception {
    ByteBuf buf = Unpooled.buffer(16);
    buf.writeInt(3);
    buf.writeFloat((float) 0.00);
    buf.writeFloat((float) 1.00);
    buf.writeFloat((float) 2.00);
    serverDenseFloatRow.update(RowType.T_FLOAT_DENSE, buf);
    DataOutputStream out = new DataOutputStream(new FileOutputStream("data"));
    serverDenseFloatRow.writeTo(out);
    out.close();
    DataInputStream in = new DataInputStream(new FileInputStream("data"));
    assertEquals(in.readFloat(), 0, 0.00);
    assertEquals(in.readFloat(), 1, 0.00);
    assertEquals(in.readFloat(), 2, 0.00);
  }

  @Test
  public void testReadFrom() throws Exception {
    ByteBuf buf = Unpooled.buffer(16);
    buf.writeInt(3);
    buf.writeFloat((float) 10.00);
    buf.writeFloat((float) 11.00);
    buf.writeFloat((float) 12.00);
    serverDenseFloatRow.update(RowType.T_FLOAT_DENSE, buf);
    DataOutputStream out = new DataOutputStream(new FileOutputStream("data"));
    serverDenseFloatRow.writeTo(out);
    out.close();
    DataInputStream in = new DataInputStream(new FileInputStream("data"));
    ServerDenseFloatRow newServerDenseFloatRow = new ServerDenseFloatRow(rowId, startCol, endCol);
    newServerDenseFloatRow.readFrom(in);

    assertEquals(newServerDenseFloatRow.getData().get(0), serverDenseFloatRow.getData().get(0),
        0.00);
    assertEquals(newServerDenseFloatRow.getData().get(1), serverDenseFloatRow.getData().get(1),
        0.00);
    assertEquals(newServerDenseFloatRow.getData().get(2), serverDenseFloatRow.getData().get(2),
        0.00);
  }

  public ServerDenseFloatRowTest() {
    super();
  }

  @Test
  public void testSerialize() throws Exception {
    ByteBuf buf = Unpooled.buffer(16);
    serverDenseFloatRow.setClock(8);
    serverDenseFloatRow.serialize(buf);
    assertEquals(serverDenseFloatRow.getRowId(), buf.readInt());
    assertEquals(serverDenseFloatRow.getClock(), buf.readInt());
    assertEquals(serverDenseFloatRow.getStartCol(), buf.readLong());
    assertEquals(serverDenseFloatRow.getEndCol(), buf.readLong());
    assertEquals(serverDenseFloatRow.getRowVersion(), buf.readInt());
    assertEquals(serverDenseFloatRow.getEndCol() - serverDenseFloatRow.getStartCol(),
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
    buf.writeFloat((float) -1.0);
    buf.writeFloat((float) -2.0);
    buf.writeFloat((float) -3.0);
    serverDenseFloatRow.deserialize(buf);
    assertEquals(serverDenseFloatRow.getRowId(), 0);
    assertEquals(serverDenseFloatRow.getClock(), 1);
    assertEquals(serverDenseFloatRow.getStartCol(), 2);
    assertEquals(serverDenseFloatRow.getEndCol(), 3);
    assertEquals(serverDenseFloatRow.getRowVersion(), 4);
    assertEquals(serverDenseFloatRow.getData().get(0), -1, 0.0);
    assertEquals(serverDenseFloatRow.getData().get(1), -2, 0.0);
    assertEquals(serverDenseFloatRow.getData().get(2), -3, 0.0);
  }

  @Test
  public void testMergeTo() throws Exception {
    ByteBuf buf = Unpooled.buffer(16);
    buf.writeInt(3);
    buf.writeFloat((float) 10.00);
    buf.writeFloat((float) 11.00);
    buf.writeFloat((float) 12.00);
    serverDenseFloatRow.update(RowType.T_FLOAT_DENSE, buf);
    float[] dataArray = {0, 1, 2, 3, 4};
    serverDenseFloatRow.mergeTo(dataArray);
    assertEquals(dataArray[0], 10, 0.00);
    assertEquals(dataArray[1], 11, 0.00);
    assertEquals(dataArray[2], 12, 0.00);
    assertEquals(dataArray[3], 3, 0.00);
    assertEquals(dataArray[4], 4, 0.00);
  }
}

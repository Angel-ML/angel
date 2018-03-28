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

import com.tencent.angel.conf.AngelConf;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.PropertyConfigurator;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DefaultRowUpdaterTest {
  private final static Log LOG = LogFactory.getLog(ServerSparseDoubleRowTest.class);
  private int rowId;
  private int startCol;
  private int endCol;
  private RowUpdater rowUpdater;

  static {
    PropertyConfigurator.configure("../conf/log4j.properties");
  }

  @Before
  public void setUp() throws Exception {
    rowId = 0;
    startCol = 0;
    endCol = 3;
    Configuration conf = new Configuration();
    Class<?> rowUpdaterClass = conf.getClass(AngelConf.ANGEL_PS_ROW_UPDATER_CLASS,
        AngelConf.DEFAULT_ANGEL_PS_ROW_UPDATER);
    rowUpdater = (RowUpdater) rowUpdaterClass.newInstance();
  }

  @Test
  public void testUpdateIntDenseToIntArbitrary() throws Exception {

  }

  @Test
  public void testUpdateIntSparseToIntArbitrary() throws Exception {

  }

  @Test
  public void testUpdateIntDenseToIntDense() throws Exception {
    ServerDenseIntRow serverDenseIntRow = new ServerDenseIntRow(rowId, startCol, endCol);
    ByteBuf buf = Unpooled.buffer(16);
    buf.writeInt(3);
    buf.writeInt(0);
    buf.writeInt(1);
    buf.writeInt(-1);
    rowUpdater.updateIntDenseToIntDense(buf, serverDenseIntRow);
    assertEquals(serverDenseIntRow.getData().get(0), 0);
    assertEquals(serverDenseIntRow.getData().get(1), 1);
    assertEquals(serverDenseIntRow.getData().get(2), -1);
  }

  @Test
  public void testUpdateIntSparseToIntDense() throws Exception {
    ServerDenseIntRow serverDenseIntRow = new ServerDenseIntRow(rowId, startCol, endCol);
    ByteBuf buf = Unpooled.buffer(16);
    buf.writeInt(3);
    buf.writeInt(0);
    buf.writeInt(0);
    buf.writeInt(1);
    buf.writeInt(1);
    buf.writeInt(2);
    buf.writeInt(-1);
    rowUpdater.updateIntSparseToIntDense(buf, serverDenseIntRow);
    assertEquals(serverDenseIntRow.getData().get(0), 0, 0.00);
    assertEquals(serverDenseIntRow.getData().get(1), 1, 0.00);
    assertEquals(serverDenseIntRow.getData().get(2), -1, 0.00);
  }

  @Test
  public void testUpdateIntDenseToIntSparse() throws Exception {
    ServerSparseIntRow serverSparseIntRow = new ServerSparseIntRow(rowId, startCol, endCol);
    ByteBuf buf = Unpooled.buffer(16);
    buf.writeInt(3);
    buf.writeInt(0);
    buf.writeInt(1);
    buf.writeInt(2);
    rowUpdater.updateIntDenseToIntSparse(buf, serverSparseIntRow);
    Int2IntOpenHashMap hashMap = new Int2IntOpenHashMap();
    hashMap.addTo(0, 0);
    hashMap.addTo(1, 1);
    hashMap.addTo(2, 2);
    assertEquals(serverSparseIntRow.getData(), hashMap);
  }

  @Test
  public void testUpdateIntSparseToIntSparse() throws Exception {
    ServerSparseIntRow serverSparseIntRow = new ServerSparseIntRow(rowId, startCol, endCol);
    ByteBuf buf = Unpooled.buffer(16);
    buf.writeInt(3);
    buf.writeInt(0);
    buf.writeInt(0);
    buf.writeInt(1);
    buf.writeInt(1);
    buf.writeInt(2);
    buf.writeInt(2);
    rowUpdater.updateIntSparseToIntSparse(buf, serverSparseIntRow);
    Int2IntOpenHashMap hashMap = new Int2IntOpenHashMap();
    hashMap.addTo(0, 0);
    hashMap.addTo(1, 1);
    hashMap.addTo(2, 2);
    assertEquals(serverSparseIntRow.getData(), hashMap);
  }

  @Test
  public void testUpdateDoubleDenseToDoubleDense() throws Exception {
    ServerDenseDoubleRow serverDenseDoubleRow = new ServerDenseDoubleRow(rowId, startCol, endCol);
    ByteBuf buf = Unpooled.buffer(16);
    buf.writeInt(3);
    buf.writeDouble(0.00);
    buf.writeDouble(1.00);
    buf.writeDouble(-1.00);
    rowUpdater.updateDoubleDenseToDoubleDense(buf, serverDenseDoubleRow);
    assertEquals(serverDenseDoubleRow.getData().get(0), 0, 0.00);
    assertEquals(serverDenseDoubleRow.getData().get(1), 1, 0.00);
    assertEquals(serverDenseDoubleRow.getData().get(2), -1, 0.00);
  }

  @Test
  public void testUpdateDoubleSparseToDoubleDense() throws Exception {
    ServerDenseDoubleRow serverDenseDoubleRow = new ServerDenseDoubleRow(rowId, startCol, endCol);
    ByteBuf buf = Unpooled.buffer(16);
    buf.writeInt(3);
    buf.writeInt(0);
    buf.writeDouble(0.00);
    buf.writeInt(1);
    buf.writeDouble(1.00);
    buf.writeInt(2);
    buf.writeDouble(-1.00);
    rowUpdater.updateDoubleSparseToDoubleDense(buf, serverDenseDoubleRow);
    assertEquals(serverDenseDoubleRow.getData().get(0), 0, 0.00);
    assertEquals(serverDenseDoubleRow.getData().get(1), 1, 0.00);
    assertEquals(serverDenseDoubleRow.getData().get(2), -1, 0.00);
  }

  @Test
  public void testUpdateDoubleDenseToDoubleSparse() throws Exception {
    ServerSparseDoubleRow serverSparseDoubleRow =
        new ServerSparseDoubleRow(rowId, startCol, endCol);
    ByteBuf buf = Unpooled.buffer(16);
    buf.writeInt(3);
    buf.writeDouble(0.00);
    buf.writeDouble(1.00);
    buf.writeDouble(2.00);
    rowUpdater.updateDoubleDenseToDoubleSparse(buf, serverSparseDoubleRow);
    Int2DoubleOpenHashMap hashMap = new Int2DoubleOpenHashMap();
    hashMap.addTo(0, 0.00);
    hashMap.addTo(1, 1.00);
    hashMap.addTo(2, 2.00);
    assertEquals(serverSparseDoubleRow.getData(), hashMap);
  }

  @Test
  public void testUpdateDoubleSparseToDoubleSparse() throws Exception {
    ServerSparseDoubleRow serverSparseDoubleRow =
        new ServerSparseDoubleRow(rowId, startCol, endCol);
    ByteBuf buf = Unpooled.buffer(16);
    buf.writeInt(3);
    buf.writeInt(0);
    buf.writeDouble(0.00);
    buf.writeInt(1);
    buf.writeDouble(1.00);
    buf.writeInt(2);
    buf.writeDouble(2.00);
    rowUpdater.updateDoubleSparseToDoubleSparse(buf, serverSparseDoubleRow);
    Int2DoubleOpenHashMap hashMap = new Int2DoubleOpenHashMap();
    hashMap.addTo(0, 0.00);
    hashMap.addTo(1, 1.00);
    hashMap.addTo(2, 2.00);
    assertEquals(serverSparseDoubleRow.getData(), hashMap);
  }

  @Test
  public void testUpdateFloatDenseToFloatDense() throws Exception {
    ServerDenseFloatRow serverDenseFloatRow = new ServerDenseFloatRow(rowId, startCol, endCol);
    ByteBuf buf = Unpooled.buffer(16);
    buf.writeInt(3);
    buf.writeFloat((float) 0.00);
    buf.writeFloat((float) 1.00);
    buf.writeFloat((float) -1.00);
    rowUpdater.updateFloatDenseToFloatDense(buf, serverDenseFloatRow);
    assertEquals(serverDenseFloatRow.getData().get(0), 0, 0.00);
    assertEquals(serverDenseFloatRow.getData().get(1), 1, 0.00);
    assertEquals(serverDenseFloatRow.getData().get(2), -1, 0.00);
  }

  @Test
  public void testUpdateDoubleDenseToDoubleDense1() throws Exception {

  }
}

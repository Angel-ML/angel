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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class ServerArbitraryIntRowTest {
  private final static Log LOG = LogFactory.getLog(ServerArbitraryIntRowTest.class);
  private ServerArbitraryIntRow serverArbitraryIntRow1;
  private ServerArbitraryIntRow serverArbitraryIntRow2;

  @Before
  public void setUp() throws Exception {
    serverArbitraryIntRow1 = new ServerArbitraryIntRow();
    serverArbitraryIntRow2 = new ServerArbitraryIntRow();

  }

  @After
  public void tearDown() throws Exception {

  }

  @Test
  public void testUpdate() throws Exception {}

  @Test
  public void testUpdateIntSparse() throws Exception {
    ByteBuf buf = Unpooled.buffer(20);
    buf.writeInt(3);
    buf.writeInt(0);
    buf.writeInt(1);
    buf.writeInt(-1);
    serverArbitraryIntRow1.updateIntDense(buf);
    serverArbitraryIntRow1.setStartCol(0);
    serverArbitraryIntRow1.setEndCol(3);
    assertNull(serverArbitraryIntRow1.getSparseRep());
    assertNotNull(serverArbitraryIntRow1.getDenseRep());
    buf = Unpooled.buffer(20);
    buf.writeInt(3);
    buf.writeInt(0);
    buf.writeInt(-1);
    buf.writeInt(-1);
    serverArbitraryIntRow1.updateIntDense(buf);
    assertNotNull(serverArbitraryIntRow1.getSparseRep());
    assertNull(serverArbitraryIntRow1.getDenseRep());
    buf = Unpooled.buffer(4 + 3 * 8);
    buf.writeInt(3);
    buf.writeInt(0);
    buf.writeInt(1);
    buf.writeInt(2);
    buf.writeInt(1);
    buf.writeInt(1);
    buf.writeInt(1);
    // LOG.info(serverArbitraryIntRow1.getSparseRep());
    serverArbitraryIntRow1.updateIntSparse(buf);
    assertNull(serverArbitraryIntRow1.getSparseRep());
    assertNotNull(serverArbitraryIntRow1.getDenseRep());
    buf = Unpooled.buffer(4 + 3 * 8);
    buf.writeInt(3);
    buf.writeInt(0);
    buf.writeInt(1);
    buf.writeInt(2);
    buf.writeInt(-1);
    buf.writeInt(-1);
    buf.writeInt(-1);
    serverArbitraryIntRow1.updateIntSparse(buf);
    assertNotNull(serverArbitraryIntRow1.getSparseRep());
    assertNull(serverArbitraryIntRow1.getDenseRep());
  }

  @Test
  public void testUpdateIntDense() throws Exception {
    ByteBuf buf = Unpooled.buffer(20);
    buf.writeInt(3);
    buf.writeInt(0);
    buf.writeInt(1);
    buf.writeInt(-1);
    serverArbitraryIntRow1.updateIntDense(buf);
    assertEquals(serverArbitraryIntRow1.get(0), 0);
    assertEquals(serverArbitraryIntRow1.get(1), 1);
    assertEquals(serverArbitraryIntRow1.get(2), -1);
    assertNull(serverArbitraryIntRow1.getSparseRep());
    buf = Unpooled.buffer(20);
    buf.writeInt(3);
    buf.writeInt(0);
    buf.writeInt(-1);
    buf.writeInt(0);
    serverArbitraryIntRow1.updateIntDense(buf);
    assertNotNull(serverArbitraryIntRow1.getSparseRep());
    assertNull(serverArbitraryIntRow1.getDenseRep());
    buf = Unpooled.buffer(20);
    buf.writeInt(3);
    buf.writeInt(0);
    buf.writeInt(-1);
    buf.writeInt(0);
    serverArbitraryIntRow1.updateIntDense(buf);
    assertNull(serverArbitraryIntRow1.getSparseRep());
  }

  @Test
  public void testEncode() throws Exception {
    serverArbitraryIntRow1.setStartCol(0);
    serverArbitraryIntRow1.setEndCol(3);
    ByteBuf bufIn = Unpooled.buffer(16);
    bufIn.writeInt(0);
    bufIn.writeInt(1);
    bufIn.writeInt(-1);
    ByteBuf bufOut = Unpooled.buffer(16);
    serverArbitraryIntRow1.encode(bufIn, bufOut, 1);
    assertEquals(0, bufOut.readInt());
    ByteBuf buf = Unpooled.buffer(20);
    buf.writeInt(3);
    buf.writeInt(0);
    buf.writeInt(1);
    buf.writeInt(-1);
    serverArbitraryIntRow1.updateIntDense(buf);
    serverArbitraryIntRow1.encode(bufIn, bufOut, 1);
    assertNull(serverArbitraryIntRow1.getSparseRep());
    assertNotNull(serverArbitraryIntRow1.getDenseRep());
    bufIn = Unpooled.buffer(20);
    bufIn.writeInt(0);
    bufIn.writeInt(1);
    bufIn.writeInt(2);
    bufOut = Unpooled.buffer(16);
    serverArbitraryIntRow1.encode(bufIn, bufOut, 3);
    assertEquals(0, bufOut.readInt());
    assertEquals(1, bufOut.readInt());
    assertEquals(-1, bufOut.readInt());
    buf = Unpooled.buffer(20);
    buf.writeInt(3);
    buf.writeInt(0);
    buf.writeInt(-1);
    buf.writeInt(0);
    serverArbitraryIntRow1.updateIntDense(buf);
    assertNotNull(serverArbitraryIntRow1.getSparseRep());
    assertNull(serverArbitraryIntRow1.getDenseRep());
    // LOG.info(serverArbitraryIntRow1.getSparseRep());
    bufIn = Unpooled.buffer(16);
    bufIn.writeInt(2);
    bufOut = Unpooled.buffer(16);
    serverArbitraryIntRow1.encode(bufIn, bufOut, 1);
    assertEquals(-1, bufOut.readInt());
  }

}

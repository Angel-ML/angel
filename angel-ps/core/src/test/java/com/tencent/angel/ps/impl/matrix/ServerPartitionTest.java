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
import com.tencent.angel.client.AngelClient;
import com.tencent.angel.client.AngelClientFactory;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.conf.MatrixConf;
import com.tencent.angel.master.DummyTask;
import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.model.output.format.ModelPartitionMeta;
import com.tencent.angel.ps.PSAttemptId;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.worker.WorkerAttemptId;
import com.tencent.angel.worker.WorkerGroupId;
import com.tencent.angel.worker.WorkerId;
import com.tencent.angel.worker.task.TaskId;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.log4j.PropertyConfigurator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;


public class ServerPartitionTest {
  private static final Log LOG = LogFactory.getLog(ServerPartitionTest.class);
  private int partitionId;
  private int matrixId;
  private int startRow;
  private int startCol;
  private int endRow;
  private int endCol;
  private RowType rowType;
  private PartitionKey partitionKey;
  private ServerPartition serverPartition;
  private static final String LOCAL_FS = LocalFileSystem.DEFAULT_FS;
  private static final String TMP_PATH = System.getProperty("java.io.tmpdir", "/tmp");
  private static AngelClient angelClient;
  private static WorkerGroupId group0Id;
  private static WorkerId worker0Id;
  private static WorkerAttemptId worker0Attempt0Id;
  private static TaskId task0Id;
  private static TaskId task1Id;
  private static ParameterServerId psId;
  private static PSAttemptId psAttempt0Id;
  private static Configuration conf;

  static {
    PropertyConfigurator.configure("../conf/log4j.properties");
  }

  @Before
  public void setUp() throws Exception {
    partitionId = 1;
    matrixId = 1;
    startRow = 2;
    startCol = 2;
    endRow = 8;
    endCol = 10;
    rowType = RowType.T_DOUBLE_DENSE;
    partitionKey = new PartitionKey(partitionId, matrixId, startRow, startCol, endRow, endCol);
    serverPartition = new ServerPartition(partitionKey, rowType, 0.0);
    serverPartition.init();
  }

  @After
  public void tearDown() throws Exception {
    File file = new File("data");
    if (file.isFile() && file.exists())
      file.delete();
  }

  @Test
  public void testGetRow() throws Exception {
    int rowIndex = 6;
    ServerRow serverRow = serverPartition.getRow(rowIndex);
    assertEquals(startCol, serverRow.getStartCol());
    assertEquals(endCol, serverRow.getEndCol());
    assertEquals(rowIndex, serverRow.getRowId());
    assertEquals(rowType, serverRow.getRowType());
  }

  @Test
  public void testGetPartitionKey() throws Exception {
    assertEquals(partitionKey, serverPartition.getPartitionKey());
  }

  @Test
  public void testClock() throws Exception {}

  @Test
  public void testGetClock() throws Exception {}

  @Test
  public void testReadFrom() throws Exception {
    // test this func in testWriteTo
  }

  @Test
  public void testWriteTo() throws Exception {
    // set basic configuration keys
    conf = new Configuration();
    conf.setBoolean("mapred.mapper.new-api", true);
    conf.setBoolean(AngelConf.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST, true);
    conf.set(AngelConf.ANGEL_TASK_USER_TASKCLASS, DummyTask.class.getName());

    // use local deploy mode and dummy dataspliter
    conf.set(AngelConf.ANGEL_DEPLOY_MODE, "LOCAL");
    conf.setBoolean(AngelConf.ANGEL_AM_USE_DUMMY_DATASPLITER, true);
    conf.set(AngelConf.ANGEL_INPUTFORMAT_CLASS, CombineTextInputFormat.class.getName());
    conf.set(AngelConf.ANGEL_SAVE_MODEL_PATH, LOCAL_FS + TMP_PATH + "/out");
    conf.set(AngelConf.ANGEL_TRAIN_DATA_PATH, LOCAL_FS + TMP_PATH + "/in");
    conf.set(AngelConf.ANGEL_LOG_PATH, LOCAL_FS + TMP_PATH + "/log");

    conf.setInt(AngelConf.ANGEL_WORKERGROUP_NUMBER, 1);
    conf.setInt(AngelConf.ANGEL_PS_NUMBER, 1);
    conf.setInt(AngelConf.ANGEL_WORKER_TASK_NUMBER, 2);

    // get a angel client
    angelClient = AngelClientFactory.get(conf);

    // add matrix
    MatrixContext mMatrix = new MatrixContext();
    mMatrix.setName("w1");
    mMatrix.setRowNum(1);
    mMatrix.setColNum(100000);
    mMatrix.setMaxRowNumInBlock(1);
    mMatrix.setMaxColNumInBlock(50000);
    mMatrix.setRowType(RowType.T_INT_DENSE);
    mMatrix.set(MatrixConf.MATRIX_OPLOG_ENABLEFILTER, "false");
    mMatrix.set(MatrixConf.MATRIX_HOGWILD, "true");
    mMatrix.set(MatrixConf.MATRIX_AVERAGE, "false");
    mMatrix.set(MatrixConf.MATRIX_OPLOG_TYPE, "DENSE_INT");
    angelClient.addMatrix(mMatrix);

    angelClient.startPSServer();
    angelClient.runTask(DummyTask.class);
    Thread.sleep(5000);

    group0Id = new WorkerGroupId(0);
    worker0Id = new WorkerId(group0Id, 0);
    worker0Attempt0Id = new WorkerAttemptId(worker0Id, 0);
    task0Id = new TaskId(0);
    task1Id = new TaskId(1);
    psId = new ParameterServerId(0);
    psAttempt0Id = new PSAttemptId(psId, 0);

    DataOutputStream out = new DataOutputStream(new FileOutputStream("data"));
    ByteBuf buf = Unpooled.buffer(4 + 8 * 8);
    buf.writeInt(8);
    buf.writeDouble(0.00);
    buf.writeDouble(1.00);
    buf.writeDouble(-1.00);
    buf.writeDouble(-2.00);
    buf.writeDouble(-5.00);
    buf.writeDouble(-6.00);
    buf.writeDouble(-7.00);
    buf.writeDouble(-8.00);
    serverPartition.getRow(6).update(RowType.T_DOUBLE_DENSE, buf);
    serverPartition.save(out);
    out.close();
    DataInputStream in = new DataInputStream(new FileInputStream("data"));
    PartitionKey partitionKeyNew = new PartitionKey(2, 1, 1, 2, 8, 10);
    ServerPartition serverPartitionNew =
        new ServerPartition(partitionKeyNew, RowType.T_DOUBLE_DENSE, 0.0);
    serverPartitionNew.init();
    assertNotEquals(((ServerDenseDoubleRow) serverPartition.getRow(6)).getData(),
        ((ServerDenseDoubleRow) serverPartitionNew.getRow(6)).getData());
    serverPartitionNew.load(in);
    in.close();
    assertEquals(((ServerDenseDoubleRow) serverPartition.getRow(6)).getData(),
        ((ServerDenseDoubleRow) serverPartitionNew.getRow(6)).getData());
    angelClient.stop();
  }

  /*@Test
  public void testReset() throws Exception {
    ServerDenseDoubleRow serverDenseDoubleRowNew = (ServerDenseDoubleRow) serverPartition.getRow(6);
    assertEquals(serverDenseDoubleRowNew, serverPartition.getRow(6));
    serverPartition.reset();
    assertNotEquals(serverDenseDoubleRowNew, serverPartition.getRow(6));
  }*/

  @Test
  public void testCommit() throws Exception {
    DataOutputStream out = new DataOutputStream(new FileOutputStream("data"));
    serverPartition.save(out, new ModelPartitionMeta());
    out.close();
    DataInputStream in = new DataInputStream(new FileInputStream("data"));
    assertEquals(partitionKey.getEndRow() - partitionKey.getStartRow(), in.readInt());
    in.close();
  }

  @Test
  public void testSerialize() throws Exception {
    ByteBuf buf = Unpooled.buffer(16);
    serverPartition.serialize(buf);
    assertEquals(partitionKey.getMatrixId(), buf.readInt());
    assertEquals(partitionKey.getPartitionId(), buf.readInt());
    assertEquals(partitionKey.getStartRow(), buf.readInt());
    assertEquals(partitionKey.getEndRow(), buf.readInt());
    assertEquals(partitionKey.getStartCol(), buf.readLong());
    assertEquals(partitionKey.getEndCol(), buf.readLong());

    assertEquals(rowType.getNumber(), buf.readInt());
    assertEquals(partitionKey.getEndRow() - partitionKey.getStartRow(), buf.readInt());
  }

  @Test
  public void testDeserialize() throws Exception {
    ByteBuf buf = Unpooled.buffer(16);
    serverPartition.serialize(buf);
    PartitionKey partitionKeyNew = new PartitionKey(2, 1, 1, 2, 8, 10);
    ServerPartition serverPartitionNew =
        new ServerPartition(partitionKeyNew, RowType.T_DOUBLE_DENSE, 0.0);
    assertNotEquals(serverPartition.getPartitionKey().getPartitionId(),
        serverPartitionNew.getPartitionKey().getPartitionId());
    serverPartitionNew.deserialize(buf);
    assertEquals(serverPartition.getPartitionKey().getPartitionId(),
        serverPartitionNew.getPartitionKey().getPartitionId());
  }

  @Test
  public void testBufferLen() throws Exception {
    assertEquals(serverPartition.bufferLen(), 592);
  }

  @Test
  public void testGetRows() throws Exception {
    List<Integer> rowsIndexes = new ArrayList<Integer>(Arrays.asList(3, 4, 5));
    List<ServerRow> rows = serverPartition.getRows(rowsIndexes);
    assertEquals(rows.size(), rowsIndexes.size());
    assertEquals(rows.get(2).getRowId(), 2);
  }

  @Test
  public void testUpdate() throws Exception {
    ServerRow rowSplitOne = new ServerDenseDoubleRow(5, 3, 4);
    rowSplitOne.setRowVersion(5);
    assertEquals(2, serverPartition.getRow(5).getStartCol());
    serverPartition.update(rowSplitOne);
    assertEquals(3, serverPartition.getRow(5).getStartCol());
  }

  @Test
  public void testUpdate1() throws Exception {
    ServerRow rowSplitOne = new ServerDenseDoubleRow(5, 3, 4);
    rowSplitOne.setRowVersion(5);
    ServerRow rowSplitTwo = new ServerDenseDoubleRow(6, 3, 4);
    rowSplitTwo.setRowVersion(6);
    List<ServerRow> rowsSplit = new ArrayList<>(Arrays.asList(rowSplitOne, rowSplitTwo));
    assertEquals(2, serverPartition.getRow(5).getStartCol());
    serverPartition.update(rowsSplit);
    assertEquals(3, serverPartition.getRow(5).getStartCol());
  }
}

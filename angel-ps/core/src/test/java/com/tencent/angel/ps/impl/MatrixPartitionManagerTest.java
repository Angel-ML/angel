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

package com.tencent.angel.ps.impl;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.client.AngelClient;
import com.tencent.angel.client.AngelClientFactory;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.conf.MatrixConf;
import com.tencent.angel.localcluster.LocalClusterContext;
import com.tencent.angel.master.DummyTask;
import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.protobuf.generated.MLProtos;
import com.tencent.angel.ps.PSAttemptId;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.ps.impl.matrix.ServerMatrix;
import com.tencent.angel.ps.impl.matrix.ServerPartition;
import com.tencent.angel.ps.impl.matrix.ServerRow;
import com.tencent.angel.worker.WorkerAttemptId;
import com.tencent.angel.worker.WorkerGroupId;
import com.tencent.angel.worker.WorkerId;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
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
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.*;


public class MatrixPartitionManagerTest {
  private static final Log LOG = LogFactory.getLog(MatrixPartitionManagerTest.class);
  private static final String LOCAL_FS = LocalFileSystem.DEFAULT_FS;
  private static final String TMP_PATH = System.getProperty("java.io.tmpdir", "/tmp");
  private AngelClient angelClient;
  private ParameterServerId psId;
  private PSAttemptId psAttempt0Id;
  private MatrixPartitionManager matrixPartitionManager;
  private ParameterServer ps;
  private WorkerGroupId group0Id;
  private WorkerId worker0Id;
  private WorkerAttemptId worker0Attempt0Id;
  private int matrixw1Id;

  static {
    PropertyConfigurator.configure("../conf/log4j.properties");
  }

  @Before
  public void setUp() throws Exception {
    try {
      // set basic configuration keys
      Configuration conf = new Configuration();
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
      mMatrix.setRowType(MLProtos.RowType.T_INT_DENSE);
      mMatrix.set(MatrixConf.MATRIX_OPLOG_ENABLEFILTER, "false");
      mMatrix.set(MatrixConf.MATRIX_HOGWILD, "true");
      mMatrix.set(MatrixConf.MATRIX_AVERAGE, "false");
      mMatrix.set(MatrixConf.MATRIX_OPLOG_TYPE, "DENSE_INT");
      angelClient.addMatrix(mMatrix);

      angelClient.startPSServer();
      angelClient.run();
      LOG.info("start angelClient");
      Thread.sleep(5000);
      psId = new ParameterServerId(0);
      psAttempt0Id = new PSAttemptId(psId, 0);
      ps = LocalClusterContext.get().getPS(psAttempt0Id).getPS();
      group0Id = new WorkerGroupId(0);
      worker0Id = new WorkerId(group0Id, 0);
      worker0Attempt0Id = new WorkerAttemptId(worker0Id, 0);
      matrixPartitionManager = ps.getMatrixPartitionManager();
      matrixw1Id = LocalClusterContext.get().getMaster().getAppMaster().getAppContext().getMatrixMetaManager().getMatrix("w1").getId();
      LOG.info("matrixPartitionManager=" + matrixPartitionManager);
    } catch (Exception x) {
      LOG.error("setup failed ", x);
      throw x;
    }
  }

  @Test
  public void testWriteMatrix() throws Exception {
    try {
      matrixPartitionManager.getMatrixIdMap().put(5, matrixPartitionManager.getMatrixIdMap().get(matrixw1Id));
      DataOutputStream out = new DataOutputStream(new FileOutputStream("data"));
      matrixPartitionManager.writeSnapshot(out);
      out.close();
      DataInputStream in = new DataInputStream(new FileInputStream("data"));
      matrixPartitionManager.readSnapshot(in);
      assertNotNull(matrixPartitionManager);

      File file = new File("data");
      if (file.isFile() && file.exists())
        file.delete();
    } catch (Exception x) {
      LOG.error("run testWriteMatrix failed ", x);
      throw x;
    }
  }

  @Test
  public void testMatrixPartitionManager() throws Exception {
    try {
      ServerRow serverRow = matrixPartitionManager.getRow(matrixw1Id, 0, 0);
      assertNotNull(serverRow);

      serverRow = matrixPartitionManager
        .getRow(matrixPartitionManager.getPartition(matrixw1Id, 1).getPartitionKey(), 0);
      assertEquals(serverRow.getRowId(), 0);

      assertTrue(matrixPartitionManager
        .partitionReady(matrixPartitionManager.getPartition(matrixw1Id, 0).getPartitionKey(), 0));

      ServerPartition serverPartition = matrixPartitionManager.getPartition(matrixw1Id, 0);
      assertNotNull(serverPartition);

      ConcurrentHashMap<Integer, ServerMatrix> matrixIdMap = matrixPartitionManager.getMatrixIdMap();
      ServerMatrix serverMatrix = matrixIdMap.get(matrixw1Id);
      assertEquals(serverMatrix.getName(), "w1");

      matrixPartitionManager.clock(matrixPartitionManager.getPartition(matrixw1Id, 1).getPartitionKey(), 0, 3);
      matrixPartitionManager.clock(matrixPartitionManager.getPartition(matrixw1Id, 1).getPartitionKey(), 1, 5);
      Object2IntOpenHashMap<PartitionKey> clocks = new Object2IntOpenHashMap();
      matrixPartitionManager.getClocks(clocks);
      assertEquals(clocks.get(matrixPartitionManager.getPartition(matrixw1Id, 1).getPartitionKey()).intValue(),
        3);

      matrixPartitionManager.clear();
      assertEquals(matrixPartitionManager.getMatrixIdMap().size(), 0);
    } catch (Exception x) {
      LOG.error("run testMatrixPartitionManager failed ", x);
      throw x;
    }
  }

  @After
  public void stop() throws Exception{
    try {
      LOG.info("stop local cluster");
      angelClient.stop();
    } catch (Exception x) {
      LOG.error("stop failed ", x);
      throw x;
    }
  }
}

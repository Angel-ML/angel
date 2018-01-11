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

package com.tencent.angel.master;

import com.tencent.angel.client.AngelClient;
import com.tencent.angel.client.AngelClientFactory;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.conf.MatrixConf;
import com.tencent.angel.localcluster.LocalClusterContext;
import com.tencent.angel.ml.math.vector.DenseIntVector;
import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ps.PSAttemptId;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.ps.impl.MatrixStorageManager;
import com.tencent.angel.ps.impl.ParameterServer;
import com.tencent.angel.ps.impl.matrix.ServerDenseIntRow;
import com.tencent.angel.ps.impl.matrix.ServerMatrix;
import com.tencent.angel.psagent.matrix.MatrixClient;
import com.tencent.angel.worker.Worker;
import com.tencent.angel.worker.WorkerAttemptId;
import com.tencent.angel.worker.WorkerGroupId;
import com.tencent.angel.worker.WorkerId;
import com.tencent.angel.worker.task.TaskContext;
import com.tencent.angel.worker.task.TaskId;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.log4j.PropertyConfigurator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.IntBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class PeriodHATest {
  private static final Log LOG = LogFactory.getLog(AppTest.class);
  private static final String LOCAL_FS = LocalFileSystem.DEFAULT_FS;
  private static final String TMP_PATH = System.getProperty("java.io.tmpdir", "/tmp");
  private AngelClient angelClient;
  private final int dim = 100;
  private TaskId task0Id;

  static {
    PropertyConfigurator.configure("../conf/log4j.properties");
  }

  @Before
  public void setup() throws Exception {
    try{
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
      conf.setInt(AngelConf.ANGEL_PS_NUMBER, 2);
      conf.setInt(AngelConf.ANGEL_WORKER_TASK_NUMBER, 1);
      conf.setInt(AngelConf.ANGEL_PS_HA_REPLICATION_NUMBER, 2);
      conf.setInt(AngelConf.ANGEL_PS_HA_PUSH_INTERVAL_MS, 1000);
      //conf.setBoolean(AngelConf.ANGEL_PS_HA_USE_EVENT_PUSH, true);
      //conf.setBoolean(AngelConf.ANGEL_PS_HA_PUSH_SYNC, true);

      // get a angel client
      angelClient = AngelClientFactory.get(conf);

      // add matrix
      MatrixContext mMatrix = new MatrixContext();
      mMatrix.setName("w1");
      mMatrix.setRowNum(1);
      mMatrix.setColNum(dim);
      mMatrix.setMaxRowNumInBlock(1);
      mMatrix.setMaxColNumInBlock(dim / 2);
      mMatrix.setRowType(RowType.T_INT_DENSE);
      mMatrix.set(MatrixConf.MATRIX_HOGWILD, "true");
      mMatrix.set(MatrixConf.MATRIX_AVERAGE, "false");
      angelClient.addMatrix(mMatrix);

      angelClient.startPSServer();
      angelClient.run();
      Thread.sleep(5000);
      task0Id = new TaskId(0);
    } catch (Exception x) {
      LOG.error("setup failed ", x);
      throw x;
    }
  }

  @Test
  public void testHA() throws Exception {
    ParameterServerId ps1Id = new ParameterServerId(0);
    final ParameterServerId ps2Id = new ParameterServerId(1);
    PSAttemptId ps1Attempt0Id = new PSAttemptId(ps1Id, 0);
    PSAttemptId ps2Attempt0Id = new PSAttemptId(ps2Id, 0);
    PSAttemptId ps2Attempt1Id = new PSAttemptId(ps2Id, 1);
    ParameterServer ps1Attempt0 = LocalClusterContext.get().getPS(ps1Attempt0Id).getPS();
    ParameterServer ps2Attempt0 = LocalClusterContext.get().getPS(ps2Attempt0Id).getPS();

    WorkerId worker0Id = new WorkerId(new WorkerGroupId(0),0);
    WorkerAttemptId worker0Attempt0Id = new WorkerAttemptId(worker0Id, 0);
    Worker worker0 = LocalClusterContext.get().getWorker(worker0Attempt0Id).getWorker();

    TaskContext task0Context = worker0.getTaskManager().getRunningTask().get(task0Id).getTaskContext();
    MatrixClient matrixClient = task0Context.getMatrix("w1");
    int iterNum = 20;

    for(int i = 0; i < iterNum; i++) {
      DenseIntVector update = new DenseIntVector(dim);
      for(int j = 0; j < dim; j++) {
        update.set(j, 1);
      }
      update.setMatrixId(matrixClient.getMatrixId());
      update.setRowId(0);

      matrixClient.increment(update);
      matrixClient.clock().get();
      Thread.sleep(1000);

      MatrixStorageManager ps1Storage = ps1Attempt0.getMatrixStorageManager();
      ServerMatrix ps1w1 = ps1Storage.getMatrix(matrixClient.getMatrixId());
      assertNotNull(ps1w1.getPartition(0));
      assertNotNull(ps1w1.getPartition(1));
      IntBuffer row0Part0 = ((ServerDenseIntRow) ps1w1.getRow(0, 0)).getData();
      int part0Size = ps1w1.getRow(0, 0).size();
      IntBuffer row0Part1 = ((ServerDenseIntRow) ps1w1.getRow(1, 0)).getData();
      int part1Size = ps1w1.getRow(1, 0).size();
      assertEquals (sum(row0Part0, part0Size), (i + 1) * dim / 2);
      assertEquals (sum(row0Part1, part1Size), (i + 1) * dim / 2);

      MatrixStorageManager ps2Storage = ps2Attempt0.getMatrixStorageManager();
      ServerMatrix ps2w1 = ps2Storage.getMatrix(matrixClient.getMatrixId());
      assertNotNull(ps2w1.getPartition(0));
      assertNotNull(ps2w1.getPartition(1));

      row0Part0 = ((ServerDenseIntRow) ps2w1.getRow(0, 0)).getData();
      part0Size = ps2w1.getRow(0, 0).size();
      row0Part1 = ((ServerDenseIntRow) ps2w1.getRow(1, 0)).getData();
      part1Size = ps2w1.getRow(1, 0).size();
      assertEquals (sum(row0Part0, part0Size), (i + 1) * dim / 2);
      assertEquals (sum(row0Part1, part1Size), (i + 1) * dim / 2);
    }

    LOG.info("===================================================================ps2 failed");
    ps2Attempt0.failed("exit");

    for(int i = iterNum; i < 2 * iterNum; i++) {
      DenseIntVector update = new DenseIntVector(dim);
      for(int j = 0; j < dim; j++) {
        update.set(j, 1);
      }
      update.setMatrixId(matrixClient.getMatrixId());
      update.setRowId(0);

      matrixClient.increment(update);
      matrixClient.clock().get();
      Thread.sleep(1000);

      MatrixStorageManager ps1Storage = ps1Attempt0.getMatrixStorageManager();
      ServerMatrix ps1w1 = ps1Storage.getMatrix(matrixClient.getMatrixId());
      assertNotNull(ps1w1.getPartition(0));
      assertNotNull(ps1w1.getPartition(1));
      IntBuffer row0Part0 = ((ServerDenseIntRow) ps1w1.getRow(0, 0)).getData();
      int part0Size = ps1w1.getRow(0, 0).size();
      IntBuffer row0Part1 = ((ServerDenseIntRow) ps1w1.getRow(1, 0)).getData();
      int part1Size = ps1w1.getRow(1, 0).size();
      assertEquals (sum(row0Part0, part0Size), (i + 1) * dim / 2);
      assertEquals (sum(row0Part1, part1Size), (i + 1) * dim / 2);
    }

    ParameterServer ps2Attempt = LocalClusterContext.get().getPS(ps2Attempt1Id).getPS();
    for(int i = iterNum * 2; i <3 * iterNum; i++) {
      DenseIntVector update = new DenseIntVector(dim);
      for(int j = 0; j < dim; j++) {
        update.set(j, 1);
      }
      update.setMatrixId(matrixClient.getMatrixId());
      update.setRowId(0);

      matrixClient.increment(update);
      matrixClient.clock().get();
      Thread.sleep(1000);

      MatrixStorageManager ps1Storage = ps1Attempt0.getMatrixStorageManager();
      ServerMatrix ps1w1 = ps1Storage.getMatrix(matrixClient.getMatrixId());
      assertNotNull(ps1w1.getPartition(0));
      assertNotNull(ps1w1.getPartition(1));
      IntBuffer row0Part0 = ((ServerDenseIntRow) ps1w1.getRow(0, 0)).getData();
      int part0Size = ps1w1.getRow(0, 0).size();
      IntBuffer row0Part1 = ((ServerDenseIntRow) ps1w1.getRow(1, 0)).getData();
      int part1Size = ps1w1.getRow(1, 0).size();
      assertEquals (sum(row0Part0, part0Size), (i + 1) * dim / 2);
      assertEquals (sum(row0Part1, part1Size), (i + 1) * dim / 2);

      MatrixStorageManager ps2Storage = ps2Attempt.getMatrixStorageManager();
      ServerMatrix ps2w1 = ps2Storage.getMatrix(matrixClient.getMatrixId());
      assertNotNull(ps2w1.getPartition(0));
      assertNotNull(ps2w1.getPartition(1));

      row0Part0 = ((ServerDenseIntRow) ps2w1.getRow(0, 0)).getData();
      part0Size = ps2w1.getRow(0, 0).size();
      row0Part1 = ((ServerDenseIntRow) ps2w1.getRow(1, 0)).getData();
      part1Size = ps2w1.getRow(1, 0).size();
      assertEquals (sum(row0Part0, part0Size), (i + 1) * dim / 2);
      assertEquals (sum(row0Part1, part1Size), (i + 1) * dim / 2);
    }
  }

  private long sum(IntBuffer buffer, int size) {
    long ret = 0L;
    for(int i = 0; i < size; i++) {
      ret += buffer.get(i);
    }

    return ret;
  }

  @After
  public void stop() throws Exception {
    try{
      LOG.info("stop local cluster");
      angelClient.stop();
    } catch (Exception x) {
      LOG.error("stop failed ", x);
      throw x;
    }
  }
}

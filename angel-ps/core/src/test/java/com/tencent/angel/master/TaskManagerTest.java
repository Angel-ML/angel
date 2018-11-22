/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in 
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */


package com.tencent.angel.master;

import com.google.protobuf.ServiceException;
import com.tencent.angel.client.AngelClient;
import com.tencent.angel.client.AngelClientFactory;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.conf.MatrixConf;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.localcluster.LocalClusterContext;
import com.tencent.angel.master.task.AMTask;
import com.tencent.angel.master.task.AMTaskManager;
import com.tencent.angel.master.worker.WorkerManager;
import com.tencent.angel.master.worker.attempt.WorkerAttempt;
import com.tencent.angel.master.worker.worker.AMWorker;
import com.tencent.angel.master.worker.workergroup.AMWorkerGroup;
import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.ml.matrix.MatrixMetaManager;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.psagent.PSAgent;
import com.tencent.angel.psagent.client.MasterClient;
import com.tencent.angel.psagent.matrix.PSAgentMatrixMetaManager;
import com.tencent.angel.worker.Worker;
import com.tencent.angel.worker.WorkerAttemptId;
import com.tencent.angel.worker.WorkerGroupId;
import com.tencent.angel.worker.WorkerId;
import com.tencent.angel.worker.task.TaskId;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.log4j.PropertyConfigurator;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TaskManagerTest {
  private static final Log LOG = LogFactory.getLog(TaskManagerTest.class);
  private static final String LOCAL_FS = LocalFileSystem.DEFAULT_FS;
  private static final String TMP_PATH = System.getProperty("java.io.tmpdir", "/tmp");
  private static AngelClient angelClient;
  private static WorkerGroupId group0Id;
  private static WorkerId worker0Id;
  private static WorkerAttemptId worker0Attempt0Id;
  private static TaskId task0Id;
  private static TaskId task1Id;

  static {
    PropertyConfigurator.configure("../conf/log4j.properties");
  }

  @BeforeClass public static void setup() throws Exception {
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
      conf.setInt(AngelConf.ANGEL_WORKER_HEARTBEAT_INTERVAL_MS, 1000);
      conf.setInt(AngelConf.ANGEL_PS_HEARTBEAT_INTERVAL_MS, 1000);


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

      MatrixContext mMatrix2 = new MatrixContext();
      mMatrix2.setName("w2");
      mMatrix2.setRowNum(1);
      mMatrix2.setColNum(100000);
      mMatrix2.setMaxRowNumInBlock(1);
      mMatrix2.setMaxColNumInBlock(50000);
      mMatrix2.setRowType(RowType.T_DOUBLE_DENSE);
      mMatrix2.set(MatrixConf.MATRIX_OPLOG_ENABLEFILTER, "false");
      mMatrix2.set(MatrixConf.MATRIX_HOGWILD, "false");
      mMatrix2.set(MatrixConf.MATRIX_AVERAGE, "false");
      mMatrix2.set(MatrixConf.MATRIX_OPLOG_TYPE, "DENSE_DOUBLE");
      angelClient.addMatrix(mMatrix2);

      angelClient.startPSServer();
      angelClient.run();
      Thread.sleep(5000);
      group0Id = new WorkerGroupId(0);
      worker0Id = new WorkerId(group0Id, 0);
      worker0Attempt0Id = new WorkerAttemptId(worker0Id, 0);
      task0Id = new TaskId(0);
      task1Id = new TaskId(1);
    } catch (Exception x) {
      LOG.error("setup failed ", x);
      throw x;
    }
  }

  @Test public void testTaskIteration() throws Exception {
    try {
      LOG.info("===========================testTaskIteration===============================");
      AngelApplicationMaster angelAppMaster = LocalClusterContext.get().getMaster().getAppMaster();
      assertTrue(angelAppMaster != null);
      AMTaskManager taskManager = angelAppMaster.getAppContext().getTaskManager();

      WorkerManager workerManager = angelAppMaster.getAppContext().getWorkerManager();
      assertTrue(workerManager != null);
      AMWorkerGroup workerGroup0 = workerManager.getWorkGroup(worker0Id);
      AMWorker worker0 = workerGroup0.getWorker(worker0Id);
      WorkerAttempt worker0Attempt0 = worker0.getWorkerAttempt(worker0Attempt0Id);

      Worker worker = LocalClusterContext.get().getWorker(worker0Attempt0Id).getWorker();
      MasterClient masterClient = worker.getPSAgent().getMasterClient();
      masterClient.taskIteration(task0Id.getIndex(), 1);
      AMTask task0 = taskManager.getTask(task0Id);
      AMTask task1 = taskManager.getTask(task1Id);
      assertEquals(task0.getIteration(), 1);
      assertEquals(task1.getIteration(), 0);
      assertEquals(worker0Attempt0.getMinIteration(), 0);
      assertEquals(worker0.getMinIteration(), 0);
      assertEquals(workerGroup0.getMinIteration(), 0);
      masterClient.taskIteration(task1Id.getIndex(), 1);
      assertEquals(task0.getIteration(), 1);
      assertEquals(task1.getIteration(), 1);
      assertEquals(worker0Attempt0.getMinIteration(), 1);
      assertEquals(worker0.getMinIteration(), 1);
      assertEquals(workerGroup0.getMinIteration(), 1);
    } catch (Exception x) {
      LOG.error("run testTaskIteration failed ", x);
      throw x;
    }
  }

  @Test public void testTaskMatrixClock() throws ServiceException {
    try {
      LOG.info("===========================testTaskMatrixClock===============================");
      AngelApplicationMaster angelAppMaster = LocalClusterContext.get().getMaster().getAppMaster();
      assertTrue(angelAppMaster != null);
      AMTaskManager taskManager = angelAppMaster.getAppContext().getTaskManager();

      Worker worker = LocalClusterContext.get().getWorker(worker0Attempt0Id).getWorker();
      PSAgentMatrixMetaManager matrixMetaManager = worker.getPSAgent().getMatrixMetaManager();
      int w1Id = matrixMetaManager.getMatrixId("w1");
      int w2Id = matrixMetaManager.getMatrixId("w2");
      MasterClient masterClient = worker.getPSAgent().getMasterClient();
      AMTask task0 = taskManager.getTask(task0Id);
      AMTask task1 = taskManager.getTask(task1Id);
      masterClient.updateClock(task0Id.getIndex(), w1Id, 1);
      masterClient.updateClock(task0Id.getIndex(), w2Id, 1);
      Int2IntOpenHashMap matrixClocks = task0.getMatrixClocks();
      assertEquals(matrixClocks.size(), 2);
      assertEquals(matrixClocks.get(w1Id), 1);
      assertEquals(matrixClocks.get(w2Id), 1);
      masterClient.updateClock(task0Id.getIndex(), w1Id, 2);
      assertEquals(task0.getMatrixClock(w1Id), 2);
      assertEquals(task0.getMatrixClock(w2Id), 1);

      masterClient.updateClock(task1Id.getIndex(), w1Id, 1);
      masterClient.updateClock(task1Id.getIndex(), w2Id, 1);
      matrixClocks = task1.getMatrixClocks();
      assertEquals(matrixClocks.size(), 2);
      assertEquals(matrixClocks.get(w1Id), 1);
      assertEquals(matrixClocks.get(w2Id), 1);
      masterClient.updateClock(task1Id.getIndex(), w1Id, 2);
      assertEquals(task1.getMatrixClock(w1Id), 2);
      assertEquals(task1.getMatrixClock(w2Id), 1);
    } catch (Exception x) {
      LOG.error("run testTaskMatrixClock failed ", x);
      throw x;
    }
  }

  @AfterClass public static void stop() throws AngelException {
    try {
      LOG.info("stop local cluster");
      angelClient.stop();
    } catch (Exception x) {
      LOG.error("stop failed ", x);
      throw x;
    }
  }
}

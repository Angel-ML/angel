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

import com.tencent.angel.PartitionKey;
import com.tencent.angel.client.AngelClient;
import com.tencent.angel.client.AngelClientFactory;
import com.tencent.angel.common.location.Location;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.conf.MatrixConf;
import com.tencent.angel.ipc.TConnection;
import com.tencent.angel.ipc.TConnectionManager;
import com.tencent.angel.localcluster.LocalClusterContext;
import com.tencent.angel.master.app.AppState;
import com.tencent.angel.master.app.InternalErrorEvent;
import com.tencent.angel.master.ps.ParameterServerManager;
import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ps.ParameterServer;
import com.tencent.angel.ps.clock.ClockVectorManager;
import com.tencent.angel.ps.storage.MatrixStorageManager;
import com.tencent.angel.ps.storage.matrix.ServerMatrix;
import com.tencent.angel.worker.task.TaskContext;
import com.tencent.angel.protobuf.ProtobufUtil;
import com.tencent.angel.protobuf.generated.MLProtos;
import com.tencent.angel.protobuf.generated.MLProtos.MatrixClock;
import com.tencent.angel.protobuf.generated.PSAgentMasterServiceProtos.TaskClockRequest;
import com.tencent.angel.protobuf.generated.PSAgentMasterServiceProtos.TaskIterationRequest;
import com.tencent.angel.ps.PSAttemptId;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.worker.Worker;
import com.tencent.angel.worker.WorkerAttemptId;
import com.tencent.angel.worker.WorkerGroupId;
import com.tencent.angel.worker.WorkerId;
import com.tencent.angel.worker.task.TaskId;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.log4j.PropertyConfigurator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MasterRecoverTest {
  private static final Log LOG = LogFactory.getLog(MasterRecoverTest.class);
  private static final String LOCAL_FS = LocalFileSystem.DEFAULT_FS;
  private static final String TMP_PATH = System.getProperty("java.io.tmpdir", "/tmp");
  private AngelClient angelClient;
  private WorkerGroupId group0Id;
  private WorkerId worker0Id;
  private WorkerAttemptId worker0Attempt0Id;
  private TaskId task0Id;
  private TaskId task1Id;
  private ParameterServerId psId;
  private PSAttemptId psAttempt0Id;

  static {
    PropertyConfigurator.configure("../conf/log4j.properties");
  }

  @Before public void setup() throws Exception {
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

      conf.setInt(AngelConf.ANGEL_AM_WRITE_STATE_INTERVAL_MS, 1000);
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
      psId = new ParameterServerId(0);
      psAttempt0Id = new PSAttemptId(psId, 0);
    } catch (Exception x) {
      LOG.error("setup failed ", x);
      throw x;
    }
  }

  @SuppressWarnings("unchecked") @Test public void testMasterRecover() throws Exception {
    try {
      ApplicationAttemptId appAttempt1Id =
        ApplicationAttemptId.newInstance(LocalClusterContext.get().getAppId(), 1);
      ApplicationAttemptId appAttempt2Id =
        ApplicationAttemptId.newInstance(LocalClusterContext.get().getAppId(), 2);

      AngelApplicationMaster angelAppMaster = LocalClusterContext.get().getMaster().getAppMaster();
      LOG.info(
        "angelAppMaster.getAppContext().getApplicationAttemptId()=" + angelAppMaster.getAppContext()
          .getApplicationAttemptId());
      assertEquals(angelAppMaster.getAppContext().getApplicationAttemptId(), appAttempt1Id);

      ParameterServerManager psManager = angelAppMaster.getAppContext().getParameterServerManager();
      ParameterServer ps = LocalClusterContext.get().getPS(psAttempt0Id).getPS();
      int w1Id = angelAppMaster.getAppContext().getMatrixMetaManager().getMatrix("w1").getId();
      int w2Id = angelAppMaster.getAppContext().getMatrixMetaManager().getMatrix("w2").getId();

      Location masterLoc =
        LocalClusterContext.get().getMaster().getAppMaster().getAppContext().getMasterService()
          .getLocation();
      TConnection connection = TConnectionManager.getConnection(ps.getConf());
      MasterProtocol master = connection.getMasterService(masterLoc.getIp(), masterLoc.getPort());

      int task0Iteration = 2;
      int task1Iteration = 1;
      int task0w1Clock = 10;
      int task0w2Clock = 20;
      int task1w1Clock = 9;
      int task1w2Clock = 19;
      int w1Clock = (task0w1Clock < task1w1Clock) ? task0w1Clock : task1w1Clock;
      int w2Clock = (task0w2Clock < task1w2Clock) ? task0w2Clock : task1w2Clock;

      master.taskIteration(null, TaskIterationRequest.newBuilder().setIteration(task0Iteration)
        .setTaskId(ProtobufUtil.convertToIdProto(task0Id)).build());
      master.taskIteration(null, TaskIterationRequest.newBuilder().setIteration(task1Iteration)
        .setTaskId(ProtobufUtil.convertToIdProto(task1Id)).build());
      master.taskClock(null,
        TaskClockRequest.newBuilder().setTaskId(ProtobufUtil.convertToIdProto(task0Id))
          .setMatrixClock(MatrixClock.newBuilder().setMatrixId(w1Id).setClock(task0w1Clock).build())
          .build());
      master.taskClock(null,
        TaskClockRequest.newBuilder().setTaskId(ProtobufUtil.convertToIdProto(task0Id))
          .setMatrixClock(MatrixClock.newBuilder().setMatrixId(w2Id).setClock(task0w2Clock).build())
          .build());
      master.taskClock(null,
        TaskClockRequest.newBuilder().setTaskId(ProtobufUtil.convertToIdProto(task1Id))
          .setMatrixClock(MatrixClock.newBuilder().setMatrixId(w1Id).setClock(task1w1Clock).build())
          .build());
      master.taskClock(null,
        TaskClockRequest.newBuilder().setTaskId(ProtobufUtil.convertToIdProto(task1Id))
          .setMatrixClock(MatrixClock.newBuilder().setMatrixId(w2Id).setClock(task1w2Clock).build())
          .build());

      int writeIntervalMS = LocalClusterContext.get().getConf()
        .getInt(AngelConf.ANGEL_AM_WRITE_STATE_INTERVAL_MS,
          AngelConf.DEFAULT_ANGEL_AM_WRITE_STATE_INTERVAL_MS);
      Thread.sleep(writeIntervalMS * 2);
      angelAppMaster.getAppContext().getEventHandler().handle(
        new InternalErrorEvent(angelAppMaster.getAppContext().getApplicationId(), "failed", true));

      Thread.sleep(10000);
      angelAppMaster = LocalClusterContext.get().getMaster().getAppMaster();
      assertEquals(angelAppMaster.getAppContext().getApp().getExternAppState(), AppState.RUNNING);
      LOG.info(
        "angelAppMaster.getAppContext().getApplicationAttemptId()=" + angelAppMaster.getAppContext()
          .getApplicationAttemptId());
      assertEquals(angelAppMaster.getAppContext().getApplicationAttemptId(), appAttempt2Id);

      PartitionKey w1Part0Key = new PartitionKey(0, w1Id, 0, 0, 1, 50000);
      PartitionKey w1Part1Key = new PartitionKey(1, w1Id, 0, 50000, 1, 100000);
      PartitionKey w2Part0Key = new PartitionKey(0, w2Id, 0, 0, 1, 50000);
      PartitionKey w2Part1Key = new PartitionKey(1, w2Id, 0, 50000, 1, 100000);

      Worker worker = LocalClusterContext.get().getWorker(worker0Attempt0Id).getWorker();
      LOG.info("worker=" + worker);
      LOG.info("worker.getTaskManager()=" + worker.getTaskManager());
      LOG.info(
        "worker.getTaskManager().getRunningTask()=" + worker.getTaskManager().getRunningTask()
          .size());

      TaskContext task0Context =
        worker.getTaskManager().getRunningTask().get(task0Id).getTaskContext();
      TaskContext task1Context =
        worker.getTaskManager().getRunningTask().get(task1Id).getTaskContext();
      assertEquals(task0Context.getEpoch(), task0Iteration);
      assertEquals(task1Context.getEpoch(), task1Iteration);
      assertEquals(task0Context.getMatrixClock(w1Id), task0w1Clock);
      assertEquals(task0Context.getMatrixClock(w2Id), task0w2Clock);
      assertEquals(task1Context.getMatrixClock(w1Id), task1w1Clock);
      assertEquals(task1Context.getMatrixClock(w2Id), task1w2Clock);

      LOG.info("===============worker.getPSAgent().getMatrixMetaManager().getMatrixMetas().size()="
        + worker.getPSAgent().getMatrixMetaManager().getMatrixMetas().size());
      assertTrue(worker.getPSAgent().getMatrixMetaManager().exist(w1Id));
      assertTrue(worker.getPSAgent().getMatrixMetaManager().exist(w2Id));

      assertEquals(worker.getPSAgent().getMatrixMetaManager().getPss(w1Part0Key).get(0), psId);
      assertEquals(worker.getPSAgent().getMatrixMetaManager().getPss(w1Part1Key).get(0), psId);
      assertEquals(worker.getPSAgent().getMatrixMetaManager().getPss(w2Part0Key).get(0), psId);
      assertEquals(worker.getPSAgent().getMatrixMetaManager().getPss(w2Part1Key).get(0), psId);

      ps = LocalClusterContext.get().getPS(psAttempt0Id).getPS();
      checkMatrixInfo(ps, w1Id, w2Id, w1Clock, w2Clock);

      angelAppMaster.getAppContext().getEventHandler().handle(
        new InternalErrorEvent(angelAppMaster.getAppContext().getApplicationId(), "failed", true));
      Thread.sleep(10000);
      angelAppMaster = LocalClusterContext.get().getMaster().getAppMaster();
      assertEquals(angelAppMaster.getAppContext().getApplicationAttemptId(), appAttempt2Id);
      assertEquals(angelAppMaster.getAppContext().getApp().getExternAppState(), AppState.FAILED);
    } catch (Exception x) {
      LOG.error("run testMasterRecover failed ", x);
      throw x;
    }
  }

  private void checkMatrixInfo(ParameterServer ps, int w1Id, int w2Id, int w1Clock, int w2Clock) {
    MatrixStorageManager matrixPartManager = ps.getMatrixStorageManager();
    ClockVectorManager clockVectorManager = ps.getClockVectorManager();
    ConcurrentHashMap<Integer, ServerMatrix> matrixIdMap = matrixPartManager.getMatrices();
    ServerMatrix sw1 = matrixIdMap.get(w1Id);
    ServerMatrix sw2 = matrixIdMap.get(w2Id);
    assertTrue(sw1 != null);
    assertTrue(sw2 != null);
    LOG.info("======================partition key is " + sw1.getPartition(0).getPartitionKey());
    LOG.info("======================partition key is " + sw1.getPartition(1).getPartitionKey());
    assertEquals(sw1.getPartition(0).getPartitionKey().getStartRow(), 0);
    assertEquals(sw1.getPartition(0).getPartitionKey().getEndRow(), 1);
    assertEquals(sw1.getPartition(0).getPartitionKey().getStartCol(), 0);
    assertEquals(sw1.getPartition(0).getPartitionKey().getEndCol(), 50000);
    assertEquals(sw1.getPartition(0).getPartitionKey().getMatrixId(), w1Id);
    assertEquals(sw1.getPartition(0).getPartitionKey().getPartitionId(), 0);
    assertEquals(clockVectorManager.getPartClock(sw1.getId(), 0), w1Clock);

    assertEquals(sw1.getPartition(1).getPartitionKey().getStartRow(), 0);
    assertEquals(sw1.getPartition(1).getPartitionKey().getEndRow(), 1);
    assertEquals(sw1.getPartition(1).getPartitionKey().getStartCol(), 50000);
    assertEquals(sw1.getPartition(1).getPartitionKey().getEndCol(), 100000);
    assertEquals(sw1.getPartition(1).getPartitionKey().getMatrixId(), w1Id);
    assertEquals(sw1.getPartition(1).getPartitionKey().getPartitionId(), 1);
    assertEquals(clockVectorManager.getPartClock(sw1.getId(), 1), w1Clock);

    assertEquals(sw2.getPartition(0).getPartitionKey().getStartRow(), 0);
    assertEquals(sw2.getPartition(0).getPartitionKey().getEndRow(), 1);
    assertEquals(sw2.getPartition(0).getPartitionKey().getStartCol(), 0);
    assertEquals(sw2.getPartition(0).getPartitionKey().getEndCol(), 50000);
    assertEquals(sw2.getPartition(0).getPartitionKey().getMatrixId(), w2Id);
    assertEquals(sw2.getPartition(0).getPartitionKey().getPartitionId(), 0);
    assertEquals(clockVectorManager.getPartClock(sw2.getId(), 0), w2Clock);

    assertEquals(sw2.getPartition(1).getPartitionKey().getStartRow(), 0);
    assertEquals(sw2.getPartition(1).getPartitionKey().getEndRow(), 1);
    assertEquals(sw2.getPartition(1).getPartitionKey().getStartCol(), 50000);
    assertEquals(sw2.getPartition(1).getPartitionKey().getEndCol(), 100000);
    assertEquals(sw2.getPartition(1).getPartitionKey().getMatrixId(), w2Id);
    assertEquals(sw2.getPartition(1).getPartitionKey().getPartitionId(), 1);
    assertEquals(clockVectorManager.getPartClock(sw2.getId(), 1), w2Clock);
  }

  @After public void stop() throws Exception {
    try {
      LOG.info("stop local cluster");
      angelClient.stop();
    } catch (Exception x) {
      LOG.error("stop failed ", x);
      throw x;
    }
  }
}

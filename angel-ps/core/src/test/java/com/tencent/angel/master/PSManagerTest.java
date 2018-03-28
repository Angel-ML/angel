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
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.tencent.angel.master;

import com.tencent.angel.client.AngelClient;
import com.tencent.angel.client.AngelClientFactory;
import com.tencent.angel.common.location.Location;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.conf.MatrixConf;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ipc.TConnection;
import com.tencent.angel.ipc.TConnectionManager;
import com.tencent.angel.localcluster.LocalClusterContext;
import com.tencent.angel.master.app.AppEvent;
import com.tencent.angel.master.app.AppEventType;
import com.tencent.angel.master.app.AppState;
import com.tencent.angel.master.ps.ParameterServerManager;
import com.tencent.angel.master.ps.attempt.PSAttempt;
import com.tencent.angel.master.ps.attempt.PSAttemptStateInternal;
import com.tencent.angel.master.ps.ps.AMParameterServer;
import com.tencent.angel.master.ps.ps.AMParameterServerState;
import com.tencent.angel.ml.math.vector.DenseIntVector;
import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.protobuf.ProtobufUtil;
import com.tencent.angel.protobuf.generated.MLProtos;
import com.tencent.angel.protobuf.generated.MLProtos.MatrixClock;
import com.tencent.angel.protobuf.generated.MLProtos.MatrixStatus;
import com.tencent.angel.protobuf.generated.MLProtos.Pair;
import com.tencent.angel.protobuf.generated.PSAgentMasterServiceProtos.TaskClockRequest;
import com.tencent.angel.protobuf.generated.PSAgentMasterServiceProtos.TaskIterationRequest;
import com.tencent.angel.protobuf.generated.PSMasterServiceProtos.*;
import com.tencent.angel.protobuf.generated.WorkerMasterServiceProtos.WorkerCommandProto;
import com.tencent.angel.protobuf.generated.WorkerMasterServiceProtos.WorkerDoneRequest;
import com.tencent.angel.protobuf.generated.WorkerMasterServiceProtos.WorkerDoneResponse;
import com.tencent.angel.ps.PSAttemptId;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.ps.impl.ClockVectorManager;
import com.tencent.angel.ps.impl.PSMatrixMetaManager;
import com.tencent.angel.ps.impl.ParameterServer;
import com.tencent.angel.ps.impl.matrix.ServerMatrix;
import com.tencent.angel.psagent.matrix.MatrixClient;
import com.tencent.angel.psagent.task.TaskContext;
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
import org.apache.log4j.PropertyConfigurator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.*;

public class PSManagerTest {
  private static final Log LOG = LogFactory.getLog(PSManagerTest.class);
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

  @Before
  public void setup() throws Exception {
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
      conf.setInt(AngelConf.ANGEL_PS_BACKUP_INTERVAL_MS, 5000);

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

  @Test
  public void testPSManager() throws  Exception {
    try {
      LOG.info("===========================testPSManager===============================");
      AngelApplicationMaster angelAppMaster = LocalClusterContext.get().getMaster().getAppMaster();
      assertTrue(angelAppMaster != null);
      ParameterServerManager psManager = angelAppMaster.getAppContext().getParameterServerManager();
      Map<ParameterServerId, AMParameterServer> psMap = psManager.getParameterServerMap();
      assertEquals(psMap.size(), 1);

      AMParameterServer ps = psMap.get(psId);
      assertTrue(ps != null);
      assertEquals(ps.getId(), psId);
      assertEquals(ps.getState(), AMParameterServerState.RUNNING);

      Map<PSAttemptId, PSAttempt> psAttempts = ps.getPSAttempts();
      assertEquals(psAttempts.size(), 1);
      PSAttempt psAttempt = psAttempts.get(psAttempt0Id);
      assertEquals(psAttempt.getInternalState(), PSAttemptStateInternal.RUNNING);
    } catch (Exception x) {
      LOG.error("run testPSManager failed ", x);
      throw x;
    }
  }

  @Test
  public void testPSReport() throws Exception {
    try {
      ParameterServer ps = LocalClusterContext.get().getPS(psAttempt0Id).getPS();
      Location masterLoc = ps.getMasterLocation();
      TConnection connection = TConnectionManager.getConnection(ps.getConf());
      MasterProtocol master = connection.getMasterService(masterLoc.getIp(), masterLoc.getPort());
      PSReportRequest.Builder builder = PSReportRequest.newBuilder();
      builder.setPsAttemptId(ProtobufUtil.convertToIdProto(psAttempt0Id));
      Pair.Builder pairBuilder = Pair.newBuilder();
      pairBuilder.setKey("ps_key1");
      pairBuilder.setValue("100");
      builder.addMetrics(pairBuilder.build());
      pairBuilder.setKey("ps_key2");
      pairBuilder.setValue("200");
      builder.addMetrics(pairBuilder.build());

      MatrixReportProto.Builder matrixBuilder = MatrixReportProto.newBuilder();
      ConcurrentHashMap<Integer, ServerMatrix> matrixIdMap = ps.getMatrixStorageManager().getMatrices();
      for (Entry<Integer, ServerMatrix> matrixEntry : matrixIdMap.entrySet()) {
        builder.addMatrixReports((matrixBuilder.setMatrixId(matrixEntry.getKey())
          .setMatrixName(matrixEntry.getValue().getName())));
      }

      PSReportResponse response = master.psReport(null, builder.build());
      assertEquals(response.getPsCommand(), PSCommandProto.PSCOMMAND_OK);
      assertEquals(response.getNeedCreateMatricesCount(), 0);
      assertEquals(response.getNeedReleaseMatrixIdsCount(), 0);
      AngelApplicationMaster angelAppMaster = LocalClusterContext.get().getMaster().getAppMaster();
      ParameterServerManager psManager = angelAppMaster.getAppContext().getParameterServerManager();
      AMParameterServer amPs = psManager.getParameterServer(psId);
      PSAttempt psAttempt = amPs.getPSAttempt(psAttempt0Id);
      Map<String, String> metrices = psAttempt.getMetrices();
      assertTrue(metrices.get("ps_key1").equals("100"));
      assertTrue(metrices.get("ps_key2").equals("200"));

      PSAttemptId psAttempt1Id = new PSAttemptId(psId, 1);
      builder.setPsAttemptId(ProtobufUtil.convertToIdProto(psAttempt1Id));
      response = master.psReport(null, builder.build());
      assertEquals(response.getPsCommand(), PSCommandProto.PSCOMMAND_SHUTDOWN);
    } catch (Exception x) {
      LOG.error("run testPSReport failed ", x);
      throw x;
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testPSDone() throws Exception {
    try {
      AngelApplicationMaster angelAppMaster = LocalClusterContext.get().getMaster().getAppMaster();
      ParameterServer ps = LocalClusterContext.get().getPS(psAttempt0Id).getPS();
      Location masterLoc = ps.getMasterLocation();
      TConnection connection = TConnectionManager.getConnection(ps.getConf());
      MasterProtocol master = connection.getMasterService(masterLoc.getIp(), masterLoc.getPort());

      WorkerDoneRequest workerRequest =
        WorkerDoneRequest.newBuilder()
          .setWorkerAttemptId(ProtobufUtil.convertToIdProto(worker0Attempt0Id)).build();
      WorkerDoneResponse workerResponse = master.workerDone(null, workerRequest);
      assertEquals(workerResponse.getCommand(), WorkerCommandProto.W_SUCCESS);
      Thread.sleep(5000);

      angelAppMaster.getAppContext().getEventHandler().handle(new AppEvent(AppEventType.COMMIT));

      PSDoneRequest request =
        PSDoneRequest.newBuilder().setPsAttemptId(ProtobufUtil.convertToIdProto(psAttempt0Id))
          .build();
      master.psDone(null, request);

      Thread.sleep(5000);

      ParameterServerManager psManager = angelAppMaster.getAppContext().getParameterServerManager();
      AMParameterServer amPs = psManager.getParameterServer(psId);
      PSAttempt psAttempt = amPs.getPSAttempt(psAttempt0Id);
      assertEquals(psAttempt.getInternalState(), PSAttemptStateInternal.SUCCESS);

      assertTrue(amPs.getState() == AMParameterServerState.SUCCESS);
      assertEquals(amPs.getNextAttemptNumber(), 1);
      assertNull(amPs.getRunningAttemptId());
      assertEquals(amPs.getSuccessAttemptId(), psAttempt0Id);
      assertEquals(amPs.getPSAttempts().size(), 1);
    } catch (Exception x) {
      LOG.error("run testPSDone failed ", x);
      throw x;
    }
  }

  @Test
  public void testPSError() throws Exception {
    try {
      int heartbeatInterval = LocalClusterContext.get().getConf().getInt(AngelConf.ANGEL_PS_HEARTBEAT_INTERVAL_MS,
        AngelConf.DEFAULT_ANGEL_PS_HEARTBEAT_INTERVAL_MS);
      AngelApplicationMaster angelAppMaster = LocalClusterContext.get().getMaster().getAppMaster();
      ParameterServerManager psManager = angelAppMaster.getAppContext().getParameterServerManager();
      AMParameterServer amPs = psManager.getParameterServer(psId);
      PSAttempt psAttempt0 = amPs.getPSAttempt(psAttempt0Id);
      ParameterServer ps = LocalClusterContext.get().getPS(psAttempt0Id).getPS();
      Worker worker = LocalClusterContext.get().getWorker(worker0Attempt0Id).getWorker();
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

      TaskContext task0Context = worker.getTaskManager().getRunningTask().get(task0Id).getTaskContext().getContext();
      TaskContext task1Context = worker.getTaskManager().getRunningTask().get(task1Id).getTaskContext().getContext();
      task0Context.setMatrixClock(w1Id, w1Clock);
      task1Context.setMatrixClock(w1Id, w1Clock);
      task0Context.setMatrixClock(w2Id, w2Clock);
      task1Context.setMatrixClock(w2Id, w2Clock);
      master.taskIteration(null, TaskIterationRequest.newBuilder().setIteration(task0Iteration)
        .setTaskId(ProtobufUtil.convertToIdProto(task0Id)).build());
      master.taskIteration(null, TaskIterationRequest.newBuilder().setIteration(task1Iteration)
        .setTaskId(ProtobufUtil.convertToIdProto(task1Id)).build());
      master.taskClock(
        null,
        TaskClockRequest
          .newBuilder()
          .setTaskId(ProtobufUtil.convertToIdProto(task0Id))
          .setMatrixClock(
            MatrixClock.newBuilder().setMatrixId(w1Id).setClock(task0w1Clock).build()).build());
      master.taskClock(
        null,
        TaskClockRequest
          .newBuilder()
          .setTaskId(ProtobufUtil.convertToIdProto(task0Id))
          .setMatrixClock(
            MatrixClock.newBuilder().setMatrixId(w2Id).setClock(task0w2Clock).build()).build());
      master.taskClock(
        null,
        TaskClockRequest
          .newBuilder()
          .setTaskId(ProtobufUtil.convertToIdProto(task1Id))
          .setMatrixClock(
            MatrixClock.newBuilder().setMatrixId(w1Id).setClock(task1w1Clock).build()).build());
      master.taskClock(
        null,
        TaskClockRequest
          .newBuilder()
          .setTaskId(ProtobufUtil.convertToIdProto(task1Id))
          .setMatrixClock(
            MatrixClock.newBuilder().setMatrixId(w2Id).setClock(task1w2Clock).build()).build());

      assertEquals(amPs.getMaxAttempts(), 4);
      PSAttemptId psAttempt1Id = new PSAttemptId(psId, 1);
      PSAttemptId psAttempt2Id = new PSAttemptId(psId, 2);
      PSAttemptId psAttempt3Id = new PSAttemptId(psId, 3);

      // attempt 0
      ps.stop(-1);
      PSErrorRequest request =
        PSErrorRequest.newBuilder().setPsAttemptId(ProtobufUtil.convertToIdProto(psAttempt0Id))
          .setMsg("out of memory").build();
      master.psError(null, request);
      Thread.sleep(heartbeatInterval * 2);

      PSAttempt psAttempt1 = amPs.getPSAttempt(psAttempt1Id);
      assertTrue(psAttempt1 != null);
      assertEquals(psAttempt0.getInternalState(), PSAttemptStateInternal.FAILED);
      assertEquals(psAttempt1.getInternalState(), PSAttemptStateInternal.RUNNING);
      assertEquals(amPs.getState(), AMParameterServerState.RUNNING);
      assertEquals(amPs.getNextAttemptNumber(), 2);
      assertEquals(amPs.getRunningAttemptId(), psAttempt1Id);
      assertNull(amPs.getSuccessAttemptId());
      assertEquals(amPs.getPSAttempts().size(), 2);

      List<String> diagnostics = amPs.getDiagnostics();
      assertEquals(diagnostics.size(), 1);
      assertEquals(diagnostics.get(0), psAttempt0Id + " failed due to: out of memory");

      ps = LocalClusterContext.get().getPS(psAttempt1Id).getPS();
      ClockVectorManager clockVectorManager = ps.getClockVectorManager();
      checkMatrixInfo(ps, w1Id, w2Id, w1Clock, w2Clock);

      MatrixClient w1Task0Client = worker.getPSAgent().getMatrixClient("w1", 0);
      MatrixClient w1Task1Client = worker.getPSAgent().getMatrixClient("w1", 1);

      int matrixW1Id = w1Task0Client.getMatrixId();

      int [] delta = new int[100000];
      for(int i = 0; i < 100000; i++) {
        delta[i] = 2;
      }

      DenseIntVector deltaVec = new DenseIntVector(100000, delta);
      deltaVec.setMatrixId(matrixW1Id);
      deltaVec.setRowId(0);
      w1Task0Client.increment(deltaVec);

      deltaVec = new DenseIntVector(100000, delta);
      deltaVec.setMatrixId(matrixW1Id);
      deltaVec.setRowId(0);
      w1Task1Client.increment(deltaVec);

      w1Task0Client.clock().get();
      w1Task1Client.clock().get();
      ps = LocalClusterContext.get().getPS(psAttempt1Id).getPS();

      int snapshotInterval =
        LocalClusterContext
          .get()
          .getConf()
          .getInt(AngelConf.ANGEL_PS_BACKUP_INTERVAL_MS,
            AngelConf.DEFAULT_ANGEL_PS_BACKUP_INTERVAL_MS);

      Thread.sleep(snapshotInterval * 2);

      // attempt1
      ps.stop(-1);
      request =
        PSErrorRequest.newBuilder().setPsAttemptId(ProtobufUtil.convertToIdProto(psAttempt1Id))
          .setMsg("out of memory").build();
      master.psError(null, request);
      Thread.sleep(heartbeatInterval * 2);

      PSAttempt psAttempt2 = amPs.getPSAttempt(psAttempt2Id);
      assertTrue(psAttempt2 != null);
      assertEquals(psAttempt1.getInternalState(), PSAttemptStateInternal.FAILED);
      assertEquals(psAttempt2.getInternalState(), PSAttemptStateInternal.RUNNING);
      assertEquals(amPs.getState(), AMParameterServerState.RUNNING);
      assertEquals(amPs.getNextAttemptNumber(), 3);
      assertEquals(amPs.getRunningAttemptId(), psAttempt2Id);
      assertNull(amPs.getSuccessAttemptId());
      assertEquals(amPs.getPSAttempts().size(), 3);

      diagnostics = amPs.getDiagnostics();
      assertEquals(diagnostics.size(), 2);
      assertEquals(diagnostics.get(0), psAttempt0Id + " failed due to: out of memory");
      assertEquals(diagnostics.get(1), psAttempt1Id + " failed due to: out of memory");

      ps = LocalClusterContext.get().getPS(psAttempt2Id).getPS();
      checkMatrixInfo(ps, w1Id, w2Id, w1Clock + 1, w2Clock);

      assertEquals(sum((DenseIntVector) w1Task0Client.getRow(0)), 400000);

      // attempt1
      ps.stop(-1);
      request =
        PSErrorRequest.newBuilder().setPsAttemptId(ProtobufUtil.convertToIdProto(psAttempt2Id))
          .setMsg("out of memory").build();
      master.psError(null, request);
      Thread.sleep(heartbeatInterval * 2);

      PSAttempt psAttempt3 = amPs.getPSAttempt(psAttempt3Id);
      assertTrue(psAttempt3 != null);
      assertEquals(psAttempt2.getInternalState(), PSAttemptStateInternal.FAILED);
      assertEquals(psAttempt3.getInternalState(), PSAttemptStateInternal.RUNNING);
      assertEquals(amPs.getState(), AMParameterServerState.RUNNING);
      assertEquals(amPs.getNextAttemptNumber(), 4);
      assertEquals(amPs.getRunningAttemptId(), psAttempt3Id);
      assertNull(amPs.getSuccessAttemptId());
      assertEquals(amPs.getPSAttempts().size(), 4);

      diagnostics = amPs.getDiagnostics();
      assertEquals(diagnostics.size(), 3);
      assertEquals(diagnostics.get(0), psAttempt0Id + " failed due to: out of memory");
      assertEquals(diagnostics.get(1), psAttempt1Id + " failed due to: out of memory");
      assertEquals(diagnostics.get(2), psAttempt2Id + " failed due to: out of memory");

      ps = LocalClusterContext.get().getPS(psAttempt3Id).getPS();
      checkMatrixInfo(ps, w1Id, w2Id, w1Clock + 1, w2Clock);

      ps.stop(-1);
      request =
        PSErrorRequest.newBuilder().setPsAttemptId(ProtobufUtil.convertToIdProto(psAttempt3Id))
          .setMsg("out of memory").build();
      master.psError(null, request);
      Thread.sleep(heartbeatInterval * 2);

      assertEquals(psAttempt3.getInternalState(), PSAttemptStateInternal.FAILED);
      assertEquals(amPs.getState(), AMParameterServerState.FAILED);
      assertEquals(angelAppMaster.getAppContext().getApp().getExternAppState(), AppState.FAILED);
      assertEquals(amPs.getNextAttemptNumber(), 4);
      assertNull(amPs.getRunningAttemptId());
      assertNull(amPs.getSuccessAttemptId());
      assertEquals(amPs.getPSAttempts().size(), 4);

      diagnostics = amPs.getDiagnostics();
      assertEquals(diagnostics.size(), 4);
      assertEquals(diagnostics.get(0), psAttempt0Id + " failed due to: out of memory");
      assertEquals(diagnostics.get(1), psAttempt1Id + " failed due to: out of memory");
      assertEquals(diagnostics.get(2), psAttempt2Id + " failed due to: out of memory");
      assertEquals(diagnostics.get(3), psAttempt3Id + " failed due to: out of memory");
    } catch (Exception x) {
      LOG.error("run testPSError failed ", x);
      throw x;
    }
  }

  private void checkMatrixInfo(ParameterServer ps, int w1Id, int w2Id, int w1Clock, int w2Clock) {
    PSMatrixMetaManager matrixPartManager = ps.getMatrixMetaManager();
    ClockVectorManager clockVectorManager = ps.getClockVectorManager();

    Map<Integer, MatrixMeta> matrixIdMap = matrixPartManager.getMatrixMetas();
    MatrixMeta sw1 = matrixIdMap.get(w1Id);
    MatrixMeta sw2 = matrixIdMap.get(w2Id);
    assertNotNull(sw1);
    assertNotNull(sw2);
    assertEquals(sw1.getPartitionMeta(0).getPartitionKey().getStartRow(), 0);
    assertEquals(sw1.getPartitionMeta(0).getPartitionKey().getEndRow(), 1);
    assertEquals(sw1.getPartitionMeta(0).getPartitionKey().getStartCol(), 0);
    assertEquals(sw1.getPartitionMeta(0).getPartitionKey().getEndCol(), 50000);
    assertEquals(sw1.getPartitionMeta(0).getPartitionKey().getMatrixId(), w1Id);
    assertEquals(sw1.getPartitionMeta(0).getPartitionKey().getPartitionId(), 0);
    assertEquals(clockVectorManager.getPartClock(sw1.getId(), 0), w1Clock);

    assertEquals(sw1.getPartitionMeta(1).getPartitionKey().getStartRow(), 0);
    assertEquals(sw1.getPartitionMeta(1).getPartitionKey().getEndRow(), 1);
    assertEquals(sw1.getPartitionMeta(1).getPartitionKey().getStartCol(), 50000);
    assertEquals(sw1.getPartitionMeta(1).getPartitionKey().getEndCol(), 100000);
    assertEquals(sw1.getPartitionMeta(1).getPartitionKey().getMatrixId(), w1Id);
    assertEquals(sw1.getPartitionMeta(1).getPartitionKey().getPartitionId(), 1);
    assertEquals(clockVectorManager.getPartClock(sw1.getId(), 1), w1Clock);

    assertEquals(sw2.getPartitionMeta(0).getPartitionKey().getStartRow(), 0);
    assertEquals(sw2.getPartitionMeta(0).getPartitionKey().getEndRow(), 1);
    assertEquals(sw2.getPartitionMeta(0).getPartitionKey().getStartCol(), 0);
    assertEquals(sw2.getPartitionMeta(0).getPartitionKey().getEndCol(), 50000);
    assertEquals(sw2.getPartitionMeta(0).getPartitionKey().getMatrixId(), w2Id);
    assertEquals(sw2.getPartitionMeta(0).getPartitionKey().getPartitionId(), 0);
    assertEquals(clockVectorManager.getPartClock(sw2.getId(), 0), w2Clock);

    assertEquals(sw2.getPartitionMeta(1).getPartitionKey().getStartRow(), 0);
    assertEquals(sw2.getPartitionMeta(1).getPartitionKey().getEndRow(), 1);
    assertEquals(sw2.getPartitionMeta(1).getPartitionKey().getStartCol(), 50000);
    assertEquals(sw2.getPartitionMeta(1).getPartitionKey().getEndCol(), 100000);
    assertEquals(sw2.getPartitionMeta(1).getPartitionKey().getMatrixId(), w2Id);
    assertEquals(sw2.getPartitionMeta(1).getPartitionKey().getPartitionId(), 1);
    assertEquals(clockVectorManager.getPartClock(sw2.getId(), 1), w2Clock);
  }
  
  private int sum(DenseIntVector vec){
    int [] values = vec.getValues();
    int sum = 0;
    for(int i = 0; i < values.length; i++) {
      sum += values[i];
    }
    return sum;
  }

  @After
  public void stop() throws AngelException {
    try {
      LOG.info("stop local cluster");
      angelClient.stop();
    } catch (Exception x) {
      LOG.error("stop failed ", x);
      throw x;
    }
  }
}

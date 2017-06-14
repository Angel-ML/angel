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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.log4j.PropertyConfigurator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.protobuf.ServiceException;
import com.tencent.angel.PartitionKey;
import com.tencent.angel.client.AngelClient;
import com.tencent.angel.client.AngelClientFactory;
import com.tencent.angel.common.Location;
import com.tencent.angel.conf.AngelConfiguration;
import com.tencent.angel.conf.MatrixConfiguration;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ipc.TConnection;
import com.tencent.angel.ipc.TConnectionManager;
import com.tencent.angel.localcluster.LocalClusterContext;
import com.tencent.angel.master.app.AppState;
import com.tencent.angel.master.task.AMTask;
import com.tencent.angel.master.worker.WorkerManager;
import com.tencent.angel.master.worker.attempt.WorkerAttempt;
import com.tencent.angel.master.worker.attempt.WorkerAttemptState;
import com.tencent.angel.master.worker.worker.AMWorker;
import com.tencent.angel.master.worker.worker.AMWorkerState;
import com.tencent.angel.master.worker.workergroup.AMWorkerGroup;
import com.tencent.angel.master.worker.workergroup.AMWorkerGroupState;
import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.protobuf.ProtobufUtil;
import com.tencent.angel.protobuf.generated.MLProtos;
import com.tencent.angel.protobuf.generated.MLProtos.MatrixClock;
import com.tencent.angel.protobuf.generated.PSAgentMasterServiceProtos.TaskClockRequest;
import com.tencent.angel.protobuf.generated.PSAgentMasterServiceProtos.TaskIterationRequest;
import com.tencent.angel.protobuf.generated.WorkerMasterServiceProtos.WorkerCommandProto;
import com.tencent.angel.protobuf.generated.WorkerMasterServiceProtos.WorkerDoneRequest;
import com.tencent.angel.protobuf.generated.WorkerMasterServiceProtos.WorkerDoneResponse;
import com.tencent.angel.protobuf.generated.WorkerMasterServiceProtos.WorkerErrorRequest;
import com.tencent.angel.protobuf.generated.WorkerMasterServiceProtos.WorkerErrorResponse;
import com.tencent.angel.ps.PSAttemptId;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.worker.Worker;
import com.tencent.angel.worker.WorkerAttemptId;
import com.tencent.angel.worker.WorkerGroupId;
import com.tencent.angel.worker.WorkerId;
import com.tencent.angel.worker.task.TaskId;

public class WorkerManagerTest {
  private static final Log LOG = LogFactory.getLog(WorkerManagerTest.class);
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
    // set basic configuration keys
    Configuration conf = new Configuration();
    conf.setBoolean("mapred.mapper.new-api", true);
    conf.setBoolean(AngelConfiguration.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST, true);
    conf.set(AngelConfiguration.ANGEL_TASK_USER_TASKCLASS, DummyTask.class.getName());

    // use local deploy mode and dummy dataspliter
    conf.set(AngelConfiguration.ANGEL_DEPLOY_MODE, "LOCAL");
    conf.setBoolean(AngelConfiguration.ANGEL_AM_USE_DUMMY_DATASPLITER, true);
    conf.set(AngelConfiguration.ANGEL_INPUTFORMAT_CLASS, CombineTextInputFormat.class.getName());
    conf.set(AngelConfiguration.ANGEL_SAVE_MODEL_PATH, LOCAL_FS + TMP_PATH + "/out");
    conf.set(AngelConfiguration.ANGEL_TRAIN_DATA_PATH, LOCAL_FS + TMP_PATH + "/in");
    conf.set(AngelConfiguration.ANGEL_LOG_PATH, LOCAL_FS + TMP_PATH + "/log");

    conf.setInt(AngelConfiguration.ANGEL_WORKERGROUP_NUMBER, 1);
    conf.setInt(AngelConfiguration.ANGEL_PS_NUMBER, 1);
    conf.setInt(AngelConfiguration.ANGEL_WORKER_TASK_NUMBER, 2);

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
    mMatrix.set(MatrixConfiguration.MATRIX_OPLOG_ENABLEFILTER, "false");
    mMatrix.set(MatrixConfiguration.MATRIX_HOGWILD, "true");
    mMatrix.set(MatrixConfiguration.MATRIX_AVERAGE, "false");
    mMatrix.set(MatrixConfiguration.MATRIX_OPLOG_TYPE, "DENSE_INT");
    angelClient.addMatrix(mMatrix);

    mMatrix.setName("w2");
    mMatrix.setRowNum(1);
    mMatrix.setColNum(100000);
    mMatrix.setMaxRowNumInBlock(1);
    mMatrix.setMaxColNumInBlock(50000);
    mMatrix.setRowType(MLProtos.RowType.T_DOUBLE_DENSE);
    mMatrix.set(MatrixConfiguration.MATRIX_OPLOG_ENABLEFILTER, "false");
    mMatrix.set(MatrixConfiguration.MATRIX_HOGWILD, "false");
    mMatrix.set(MatrixConfiguration.MATRIX_AVERAGE, "false");
    mMatrix.set(MatrixConfiguration.MATRIX_OPLOG_TYPE, "DENSE_DOUBLE");
    angelClient.addMatrix(mMatrix);

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
  }

  @Test
  public void testWorkerManager() {
    LOG.info("===========================testWorkerManager===============================");
    AngelApplicationMaster angelAppMaster = LocalClusterContext.get().getMaster().getAppMaster();
    assertTrue(angelAppMaster != null);

    WorkerManager workerManager = angelAppMaster.getAppContext().getWorkerManager();
    assertTrue(workerManager != null);

    assertEquals(workerManager.getActiveTaskNum(), 2);
    assertEquals(workerManager.getActiveWorkerGroupNumber(), 1);

    Map<WorkerGroupId, AMWorkerGroup> workerGroups = workerManager.getWorkerGroupMap();
    assertTrue(workerGroups != null);
    assertEquals(workerGroups.size(), 1);


    AMWorkerGroup workerGroup0 = workerGroups.get(group0Id);
    assertTrue(workerGroup0 != null);
    assertEquals(workerGroup0.getState(), AMWorkerGroupState.RUNNING);
    assertEquals(workerGroup0.getSplitIndex(), 0);

    Map<WorkerId, AMWorker> workersInGroup0 = workerGroup0.getWorkerMap();
    AMWorker worker0 = workersInGroup0.get(worker0Id);
    assertEquals(workersInGroup0.size(), 1);
    assertTrue(worker0 != null);
    assertEquals(worker0.getState(), AMWorkerState.RUNNING);

    Map<WorkerAttemptId, WorkerAttempt> attemptsInWorker0 = worker0.getAttempts();
    WorkerAttempt worker0Attempt0 = attemptsInWorker0.get(worker0Attempt0Id);
    assertEquals(attemptsInWorker0.size(), 1);
    assertTrue(worker0Attempt0 != null);
    assertEquals(worker0Attempt0.getState(), WorkerAttemptState.RUNNING);
    assertEquals(worker0Attempt0.getMinIteration(), 0);

    Map<TaskId, AMTask> taskMap = worker0Attempt0.getTaskMap();
    assertEquals(taskMap.size(), 2);
    AMTask task0 = taskMap.get(task0Id);
    AMTask task1 = taskMap.get(task1Id);
    assertTrue(task0 != null);
    assertTrue(task1 != null);
  }

  @Test
  public void testWorkerDone() throws IOException, ServiceException, InterruptedException {
    LOG.info("===========================testWorkerDone===============================");
    Worker worker = LocalClusterContext.get().getWorker(worker0Attempt0Id).getWorker();
    Location masterLoc =
        LocalClusterContext.get().getMaster().getAppMaster().getAppContext().getMasterService()
            .getLocation();
    TConnection connection = TConnectionManager.getConnection(worker.getConf());
    MasterProtocol master = connection.getMasterService(masterLoc.getIp(), masterLoc.getPort());
    WorkerDoneRequest request =
        WorkerDoneRequest.newBuilder()
            .setWorkerAttemptId(ProtobufUtil.convertToIdProto(worker0Attempt0Id)).build();
    WorkerDoneResponse response = master.workerDone(null, request);
    assertTrue(response.getCommand() == WorkerCommandProto.W_SUCCESS);
    Thread.sleep(5000);

    AngelApplicationMaster angelAppMaster = LocalClusterContext.get().getMaster().getAppMaster();
    WorkerManager workerManager = angelAppMaster.getAppContext().getWorkerManager();
    AMWorkerGroup worker0Group = workerManager.getWorkerGroup(group0Id);
    AMWorker amWorker = worker0Group.getWorker(worker0Id);
    WorkerAttempt worker0Attempt = amWorker.getWorkerAttempt(worker0Attempt0Id);
    assertEquals(worker0Attempt.getState(), WorkerAttemptState.SUCCESS);
    assertEquals(amWorker.getState(), AMWorkerState.SUCCESS);
    assertEquals(amWorker.getSuccessAttemptId(), worker0Attempt0Id);
    assertNull(amWorker.getRunningAttempt());
    assertEquals(amWorker.getAttempts().size(), 1);
    assertEquals(amWorker.getNextAttemptNumber(), 1);
    assertEquals(amWorker.getFailedAttempts().size(), 0);
    assertEquals(worker0Group.getState(), AMWorkerGroupState.SUCCESS);

    response = master.workerDone(null, request);
    assertEquals(response.getCommand(), WorkerCommandProto.W_SHUTDOWN);

    WorkerAttemptId worker1Attempt0Id =
        new WorkerAttemptId(new WorkerId(new WorkerGroupId(1), 0), 0);
    WorkerDoneRequest registeRequest =
        WorkerDoneRequest.newBuilder()
            .setWorkerAttemptId(ProtobufUtil.convertToIdProto(worker1Attempt0Id)).build();
    response = master.workerDone(null, registeRequest);
    assertEquals(response.getCommand(), WorkerCommandProto.W_SHUTDOWN);
  }


  @Test
  public void testWorkerError() throws IOException, ServiceException, InterruptedException {
    LOG.info("===========================testWorkerError===============================");

    AngelApplicationMaster angelAppMaster = LocalClusterContext.get().getMaster().getAppMaster();
    WorkerManager workerManager = angelAppMaster.getAppContext().getWorkerManager();
    AMWorkerGroup worker0Group = workerManager.getWorkerGroup(group0Id);
    AMWorker amWorker = worker0Group.getWorker(worker0Id);
    assertEquals(amWorker.getMaxAttempts(), 4);
    WorkerAttemptId worker0Attempt1Id = new WorkerAttemptId(worker0Id, 1);
    WorkerAttemptId worker0Attempt2Id = new WorkerAttemptId(worker0Id, 2);
    WorkerAttemptId worker0Attempt3Id = new WorkerAttemptId(worker0Id, 3);

    // attempt 0
    LOG.info("===========================attempt0=================================");
    Worker worker = LocalClusterContext.get().getWorker(worker0Attempt0Id).getWorker();
    Location masterLoc =
        LocalClusterContext.get().getMaster().getAppMaster().getAppContext().getMasterService()
            .getLocation();
    TConnection connection = TConnectionManager.getConnection(worker.getConf());
    MasterProtocol master = connection.getMasterService(masterLoc.getIp(), masterLoc.getPort());
    int w1Id = angelAppMaster.getAppContext().getMatrixMetaManager().getMatrix("w1").getId();
    int w2Id = angelAppMaster.getAppContext().getMatrixMetaManager().getMatrix("w2").getId();
    int task0Iteration = 2;
    int task1Iteration = 1;
    int task0w1Clock = 10;
    int task0w2Clock = 20;
    int task1w1Clock = 9;
    int task1w2Clock = 19;

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

    Set<Integer> matrixSet = worker.getPSAgent().getMatrixMetaManager().getMatrixIds();
    assertEquals(matrixSet.size(), 2);
    assertTrue(matrixSet.contains(w1Id));
    assertTrue(matrixSet.contains(w2Id));
    PartitionKey w1Part0Key = new PartitionKey(0, w1Id, 0, 0, 1, 50000);
    PartitionKey w1Part1Key = new PartitionKey(1, w1Id, 0, 50000, 1, 100000);
    PartitionKey w2Part0Key = new PartitionKey(0, w2Id, 0, 0, 1, 50000);
    PartitionKey w2Part1Key = new PartitionKey(1, w2Id, 0, 50000, 1, 100000);

    assertEquals(worker.getPSAgent().getMatrixPartitionRouter().getPSId(w1Part0Key), psId);
    assertEquals(worker.getPSAgent().getMatrixPartitionRouter().getPSId(w1Part1Key), psId);
    assertEquals(worker.getPSAgent().getMatrixPartitionRouter().getPSId(w2Part0Key), psId);
    assertEquals(worker.getPSAgent().getMatrixPartitionRouter().getPSId(w2Part1Key), psId);

    WorkerErrorRequest request =
        WorkerErrorRequest.newBuilder()
            .setWorkerAttemptId(ProtobufUtil.convertToIdProto(worker0Attempt0Id))
            .setMsg("task0 failed:parse data failed.").build();
    worker.stop();

    WorkerErrorResponse response = master.workerError(null, request);
    assertTrue(response.getCommand() == WorkerCommandProto.W_SUCCESS);
    Thread.sleep(5000);

    WorkerAttempt worker0Attempt0 = amWorker.getWorkerAttempt(worker0Attempt0Id);
    WorkerAttempt worker0Attempt1 = amWorker.getWorkerAttempt(worker0Attempt1Id);

    assertTrue(worker0Attempt0 != null);
    assertTrue(worker0Attempt1 != null);

    assertEquals(worker0Attempt0.getState(), WorkerAttemptState.FAILED);
    List<String> attempt0Diagnostics = worker0Attempt0.getDiagnostics();
    assertEquals(attempt0Diagnostics.size(), 1);
    assertEquals(attempt0Diagnostics.get(0), "task0 failed:parse data failed.");

    assertEquals(worker0Attempt1.getState(), WorkerAttemptState.RUNNING);

    assertEquals(amWorker.getState(), AMWorkerState.RUNNING);
    assertNull(amWorker.getSuccessAttemptId());
    assertEquals(amWorker.getRunningAttempt().getId(), worker0Attempt1Id);
    assertEquals(amWorker.getAttempts().size(), 2);
    assertEquals(amWorker.getNextAttemptNumber(), 2);
    assertEquals(amWorker.getFailedAttempts().size(), 1);
    assertTrue(amWorker.getFailedAttempts().contains(worker0Attempt0Id));
    List<String> worker0Diagnostics = amWorker.getDiagnostics();
    assertEquals(worker0Diagnostics.size(), 1);
    assertEquals(worker0Diagnostics.get(0),
        "WorkerAttempt_0_0_0 failed due to: task0 failed:parse data failed.");
    assertEquals(worker0Group.getState(), AMWorkerGroupState.RUNNING);

    // attempt 1
    LOG.info("===========================attempt1=================================");
    worker = LocalClusterContext.get().getWorker(worker0Attempt1Id).getWorker();
    com.tencent.angel.worker.task.TaskContext task0Context =
        worker.getTaskManager().getRunningTask().get(task0Id).getTaskContext();
    com.tencent.angel.worker.task.TaskContext task1Context =
        worker.getTaskManager().getRunningTask().get(task1Id).getTaskContext();
    assertEquals(task0Context.getIteration(), task0Iteration);
    assertEquals(task1Context.getIteration(), task1Iteration);
    assertEquals(task0Context.getMatrixClock(w1Id), task0w1Clock);
    assertEquals(task0Context.getMatrixClock(w2Id), task0w2Clock);
    assertEquals(task1Context.getMatrixClock(w1Id), task1w1Clock);
    assertEquals(task1Context.getMatrixClock(w2Id), task1w2Clock);
    matrixSet = worker.getPSAgent().getMatrixMetaManager().getMatrixIds();
    assertEquals(matrixSet.size(), 2);
    assertTrue(matrixSet.contains(w1Id));
    assertTrue(matrixSet.contains(w2Id));
    assertEquals(worker.getPSAgent().getMatrixPartitionRouter().getPSId(w1Part0Key), psId);
    assertEquals(worker.getPSAgent().getMatrixPartitionRouter().getPSId(w1Part1Key), psId);
    assertEquals(worker.getPSAgent().getMatrixPartitionRouter().getPSId(w2Part0Key), psId);
    assertEquals(worker.getPSAgent().getMatrixPartitionRouter().getPSId(w2Part1Key), psId);

    request =
        WorkerErrorRequest.newBuilder()
            .setWorkerAttemptId(ProtobufUtil.convertToIdProto(worker0Attempt1Id))
            .setMsg("task0 failed:disk write failed.").build();
    worker.stop();
    response = master.workerError(null, request);
    assertEquals(response.getCommand(), WorkerCommandProto.W_SUCCESS);
    Thread.sleep(5000);

    WorkerAttempt worker0Attempt2 = amWorker.getWorkerAttempt(worker0Attempt2Id);
    assertTrue(worker0Attempt2 != null);

    assertEquals(worker0Attempt1.getState(), WorkerAttemptState.FAILED);
    List<String> attempt1Diagnostics = worker0Attempt1.getDiagnostics();
    assertEquals(attempt1Diagnostics.size(), 1);
    assertEquals(attempt1Diagnostics.get(0), "task0 failed:disk write failed.");

    assertEquals(worker0Attempt2.getState(), WorkerAttemptState.RUNNING);

    assertEquals(amWorker.getState(), AMWorkerState.RUNNING);
    assertNull(amWorker.getSuccessAttemptId());
    assertEquals(amWorker.getRunningAttempt().getId(), worker0Attempt2Id);
    assertEquals(amWorker.getAttempts().size(), 3);
    assertEquals(amWorker.getNextAttemptNumber(), 3);
    assertEquals(amWorker.getFailedAttempts().size(), 2);
    assertTrue(amWorker.getFailedAttempts().contains(worker0Attempt1Id));

    worker0Diagnostics = amWorker.getDiagnostics();
    assertEquals(worker0Diagnostics.size(), 2);
    assertEquals(worker0Diagnostics.get(0),
        "WorkerAttempt_0_0_0 failed due to: task0 failed:parse data failed.");
    assertEquals(worker0Diagnostics.get(1),
        "WorkerAttempt_0_0_1 failed due to: task0 failed:disk write failed.");
    assertEquals(worker0Group.getState(), AMWorkerGroupState.RUNNING);

    // attempt 2
    LOG.info("===========================attempt2=================================");
    worker = LocalClusterContext.get().getWorker(worker0Attempt2Id).getWorker();
    request =
        WorkerErrorRequest.newBuilder()
            .setWorkerAttemptId(ProtobufUtil.convertToIdProto(worker0Attempt2Id))
            .setMsg("task0 failed:disk write failed.").build();
    worker.stop();
    response = master.workerError(null, request);
    assertEquals(response.getCommand(), WorkerCommandProto.W_SUCCESS);
    Thread.sleep(5000);

    WorkerAttempt worker0Attempt3 = amWorker.getWorkerAttempt(worker0Attempt3Id);
    assertTrue(worker0Attempt3 != null);

    assertEquals(worker0Attempt2.getState(), WorkerAttemptState.FAILED);
    List<String> attempt2Diagnostics = worker0Attempt2.getDiagnostics();
    assertEquals(attempt2Diagnostics.size(), 1);
    assertEquals(attempt2Diagnostics.get(0), "task0 failed:disk write failed.");

    assertEquals(worker0Attempt3.getState(), WorkerAttemptState.RUNNING);

    assertEquals(amWorker.getState(), AMWorkerState.RUNNING);
    assertNull(amWorker.getSuccessAttemptId());
    assertEquals(amWorker.getRunningAttempt().getId(), worker0Attempt3Id);
    assertEquals(amWorker.getAttempts().size(), 4);
    assertEquals(amWorker.getNextAttemptNumber(), 4);
    assertEquals(amWorker.getFailedAttempts().size(), 3);
    assertTrue(amWorker.getFailedAttempts().contains(worker0Attempt2Id));

    worker0Diagnostics = amWorker.getDiagnostics();
    assertEquals(worker0Diagnostics.size(), 3);
    assertEquals(worker0Diagnostics.get(0),
        "WorkerAttempt_0_0_0 failed due to: task0 failed:parse data failed.");
    assertEquals(worker0Diagnostics.get(1),
        "WorkerAttempt_0_0_1 failed due to: task0 failed:disk write failed.");
    assertEquals(worker0Diagnostics.get(2),
        "WorkerAttempt_0_0_2 failed due to: task0 failed:disk write failed.");
    assertEquals(worker0Group.getState(), AMWorkerGroupState.RUNNING);

    // attempt 3
    LOG.info("===========================attempt3=================================");
    worker = LocalClusterContext.get().getWorker(worker0Attempt3Id).getWorker();
    request =
        WorkerErrorRequest.newBuilder()
            .setWorkerAttemptId(ProtobufUtil.convertToIdProto(worker0Attempt3Id))
            .setMsg("task0 failed:disk write failed.").build();
    worker.stop();
    response = master.workerError(null, request);
    assertEquals(response.getCommand(), WorkerCommandProto.W_SUCCESS);

    Thread.sleep(1000);

    assertEquals(worker0Attempt3.getState(), WorkerAttemptState.FAILED);
    List<String> attempt3Diagnostics = worker0Attempt3.getDiagnostics();
    assertEquals(attempt3Diagnostics.size(), 1);
    assertEquals(attempt3Diagnostics.get(0), "task0 failed:disk write failed.");

    assertEquals(amWorker.getState(), AMWorkerState.FAILED);
    assertNull(amWorker.getSuccessAttemptId());
    assertNull(amWorker.getRunningAttempt());
    assertEquals(amWorker.getAttempts().size(), 4);
    assertEquals(amWorker.getNextAttemptNumber(), 4);
    assertEquals(amWorker.getFailedAttempts().size(), 4);
    assertTrue(amWorker.getFailedAttempts().contains(worker0Attempt3Id));

    worker0Diagnostics = amWorker.getDiagnostics();
    assertEquals(worker0Diagnostics.size(), 4);
    assertEquals(worker0Diagnostics.get(0),
        "WorkerAttempt_0_0_0 failed due to: task0 failed:parse data failed.");
    assertEquals(worker0Diagnostics.get(1),
        "WorkerAttempt_0_0_1 failed due to: task0 failed:disk write failed.");
    assertEquals(worker0Diagnostics.get(2),
        "WorkerAttempt_0_0_2 failed due to: task0 failed:disk write failed.");
    assertEquals(worker0Diagnostics.get(3),
        "WorkerAttempt_0_0_3 failed due to: task0 failed:disk write failed.");
    assertEquals(worker0Group.getState(), AMWorkerGroupState.FAILED);
    assertEquals(angelAppMaster.getAppContext().getApp().getInternalState(), AppState.FAILED);
  }

  @After
  public void stop() throws AngelException {
    LOG.info("stop local cluster");
    angelClient.stop();
  }
}

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

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.log4j.PropertyConfigurator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.protobuf.ServiceException;
import com.tencent.angel.client.AngelClient;
import com.tencent.angel.client.AngelClientFactory;
import com.tencent.angel.common.Location;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.conf.MatrixConf;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ipc.TConnection;
import com.tencent.angel.ipc.TConnectionManager;
import com.tencent.angel.localcluster.LocalClusterContext;
import com.tencent.angel.master.task.AMTask;
import com.tencent.angel.master.task.AMTaskManager;
import com.tencent.angel.master.worker.attempt.WorkerAttempt;
import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.protobuf.ProtobufUtil;
import com.tencent.angel.protobuf.generated.MLProtos;
import com.tencent.angel.protobuf.generated.MLProtos.LocationProto;
import com.tencent.angel.protobuf.generated.MLProtos.Pair;
import com.tencent.angel.protobuf.generated.WorkerMasterServiceProtos.TaskStateProto;
import com.tencent.angel.protobuf.generated.WorkerMasterServiceProtos.WorkerCommandProto;
import com.tencent.angel.protobuf.generated.WorkerMasterServiceProtos.WorkerRegisterRequest;
import com.tencent.angel.protobuf.generated.WorkerMasterServiceProtos.WorkerRegisterResponse;
import com.tencent.angel.protobuf.generated.WorkerMasterServiceProtos.WorkerReportRequest;
import com.tencent.angel.protobuf.generated.WorkerMasterServiceProtos.WorkerReportResponse;
import com.tencent.angel.ps.PSAttemptId;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.worker.Worker;
import com.tencent.angel.worker.WorkerAttemptId;
import com.tencent.angel.worker.WorkerGroupId;
import com.tencent.angel.worker.WorkerId;
import com.tencent.angel.worker.task.TaskId;

@RunWith(MockitoJUnitRunner.class)
public class MasterServiceTest {
  private static final Log LOG = LogFactory.getLog(MasterServiceTest.class);
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

      mMatrix.setName("w2");
      mMatrix.setRowNum(1);
      mMatrix.setColNum(100000);
      mMatrix.setMaxRowNumInBlock(1);
      mMatrix.setMaxColNumInBlock(50000);
      mMatrix.setRowType(MLProtos.RowType.T_DOUBLE_DENSE);
      mMatrix.set(MatrixConf.MATRIX_OPLOG_ENABLEFILTER, "false");
      mMatrix.set(MatrixConf.MATRIX_HOGWILD, "false");
      mMatrix.set(MatrixConf.MATRIX_AVERAGE, "false");
      mMatrix.set(MatrixConf.MATRIX_OPLOG_TYPE, "DENSE_DOUBLE");
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
    } catch (Exception x) {
      LOG.error("setup failed ", x);
      throw x;
    }
  }

  @Test
  public void testMasterService() throws Exception {
    try{
      LOG.info("===========================testMasterService===============================");
      Worker worker = LocalClusterContext.get().getWorker(worker0Attempt0Id).getWorker();
      Location masterLoc =
        LocalClusterContext.get().getMaster().getAppMaster().getAppContext().getMasterService()
          .getLocation();
      TConnection connection = TConnectionManager.getConnection(worker.getConf());
      MasterProtocol master = connection.getMasterService(masterLoc.getIp(), masterLoc.getPort());

      // worker register
      WorkerAttemptId worker1Attempt0Id =
        new WorkerAttemptId(new WorkerId(new WorkerGroupId(1), 0), 0);
      WorkerRegisterRequest registeRequest =
        WorkerRegisterRequest.newBuilder()
          .setWorkerAttemptId(ProtobufUtil.convertToIdProto(worker1Attempt0Id))
          .setLocation(LocationProto.newBuilder().setIp("10.10.10.10").setPort(10000).build())
          .build();
      WorkerRegisterResponse registerResponse = master.workerRegister(null, registeRequest);
      assertTrue(registerResponse.getCommand() == WorkerCommandProto.W_SHUTDOWN);

      WorkerReportRequest.Builder reportBuilder = WorkerReportRequest.newBuilder();
      Pair.Builder kvBuilder = Pair.newBuilder();
      TaskStateProto.Builder taskBuilder = TaskStateProto.newBuilder();

      reportBuilder.setWorkerAttemptId(ProtobufUtil.convertToIdProto(worker0Attempt0Id));

      taskBuilder.setProgress(0.20f);
      taskBuilder.setState("RUNNING");
      taskBuilder.setTaskId(ProtobufUtil.convertToIdProto(task0Id));
      kvBuilder.setKey("task_key1");
      kvBuilder.setValue("100");
      taskBuilder.addCounters(kvBuilder.build());
      kvBuilder.setKey("task_key2");
      kvBuilder.setValue("200");
      taskBuilder.addCounters(kvBuilder.build());
      reportBuilder.addTaskReports(taskBuilder.build());

      taskBuilder.setProgress(0.30f);
      taskBuilder.setState("RUNNING");
      taskBuilder.setTaskId(ProtobufUtil.convertToIdProto(task1Id));
      kvBuilder.setKey("task_key1");
      kvBuilder.setValue("1000");
      taskBuilder.addCounters(kvBuilder.build());
      kvBuilder.setKey("task_key2");
      kvBuilder.setValue("2000");
      taskBuilder.addCounters(kvBuilder.build());
      reportBuilder.addTaskReports(taskBuilder.build());

      kvBuilder.setKey("worker_key1");
      kvBuilder.setValue("100");
      reportBuilder.addPairs(kvBuilder.build());
      kvBuilder.setKey("worker_key2");
      kvBuilder.setValue("200");
      reportBuilder.addPairs(kvBuilder.build());

      WorkerReportResponse reportResponse = master.workerReport(null, reportBuilder.build());
      assertTrue(reportResponse.getCommand() == WorkerCommandProto.W_SUCCESS);
      assertEquals(reportResponse.getActiveTaskNum(), 2);

      AngelApplicationMaster angelAppMaster = LocalClusterContext.get().getMaster().getAppMaster();
      WorkerAttempt worker0Attempt =
        angelAppMaster.getAppContext().getWorkerManager()
          .getWorker(worker0Attempt0Id.getWorkerId()).getWorkerAttempt(worker0Attempt0Id);
      assertTrue(worker0Attempt != null);
      Map<String, String> workerMetrics = worker0Attempt.getMetrics();
      String valueForWorkerKey1 = workerMetrics.get("worker_key1");
      String valueForWorkerKey2 = workerMetrics.get("worker_key2");
      assertNotNull(valueForWorkerKey1);
      assertNotNull(valueForWorkerKey2);
      assertEquals(valueForWorkerKey1, "100");
      assertEquals(valueForWorkerKey2, "200");

      AMTaskManager amTaskManager = angelAppMaster.getAppContext().getTaskManager();
      AMTask task0 = amTaskManager.getTask(task0Id);
      AMTask task1 = amTaskManager.getTask(task1Id);
      assertTrue(task0 != null);
      assertTrue(task1 != null);
      Map<String, String> task0Metrics = task0.getMetrics();
      Map<String, String> task1Metrics = task1.getMetrics();
      String valueForTask0Key1 = task0Metrics.get("task_key1");
      String valueForTask0Key2 = task0Metrics.get("task_key2");
      String valueForTask1Key1 = task1Metrics.get("task_key1");
      String valueForTask1Key2 = task1Metrics.get("task_key2");
      assertTrue(valueForTask0Key1 != null);
      assertTrue(valueForTask0Key2 != null);
      assertTrue(valueForTask1Key1 != null);
      assertTrue(valueForTask1Key2 != null);
      assertEquals(valueForTask0Key1, "100");
      assertEquals(valueForTask0Key2, "200");
      assertEquals(valueForTask1Key1, "1000");
      assertEquals(valueForTask1Key2, "2000");
      assertEquals(task0.getProgress(), 0.20f, 0.000001);
      assertEquals(task1.getProgress(), 0.30f, 0.000001);
    } catch (Exception x) {
      LOG.error("run testMasterService failed ", x);
      throw  x;
    }
  }

  @After
  public void stop() throws Exception {
    try {
      LOG.info("stop local cluster");
      angelClient.stop();
    } catch (Exception x) {
      LOG.error("stop failed ", x);
      throw x;
    }
  }
}

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

import com.tencent.angel.client.AngelClient;
import com.tencent.angel.client.AngelClientFactory;
import com.tencent.angel.common.location.Location;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.conf.MatrixConf;
import com.tencent.angel.ipc.TConnection;
import com.tencent.angel.ipc.TConnectionManager;
import com.tencent.angel.localcluster.LocalClusterContext;
import com.tencent.angel.master.app.InternalErrorEvent;
import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.protobuf.ProtobufUtil;
import com.tencent.angel.protobuf.generated.ClientMasterServiceProtos.GetJobReportRequest;
import com.tencent.angel.protobuf.generated.ClientMasterServiceProtos.GetJobReportResponse;
import com.tencent.angel.protobuf.generated.ClientMasterServiceProtos.JobStateProto;
import com.tencent.angel.protobuf.generated.PSAgentMasterServiceProtos.TaskIterationRequest;
import com.tencent.angel.worker.task.TaskId;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.log4j.PropertyConfigurator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class AppTest {
  private static final Log LOG = LogFactory.getLog(AppTest.class);
  private static final String LOCAL_FS = LocalFileSystem.DEFAULT_FS;
  private static final String TMP_PATH = System.getProperty("java.io.tmpdir", "/tmp");
  private AngelClient angelClient;
  private TaskId task0Id;
  private TaskId task1Id;

  static {
    PropertyConfigurator.configure("../conf/log4j.properties");
  }

  @Before public void setup() throws Exception {
    try {
      // set basic configuration keys
      Configuration conf = new Configuration();
      conf.setBoolean("mapred.mapper.new-api", true);
      conf.setBoolean(AngelConf.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST, true);
      //conf.set(AngelConf.ANGEL_TASK_USER_TASKCLASS, DummyTask.class.getName());

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

      angelClient.startPSServer();
      angelClient.runTask(DummyTask.class);
      Thread.sleep(5000);
      task0Id = new TaskId(0);
      task1Id = new TaskId(1);
    } catch (Exception x) {
      LOG.error("setup failed ", x);
      throw x;
    }
  }

  @SuppressWarnings("unchecked") @Test public void testGetJobReport() throws Exception {
    try {
      AngelApplicationMaster angelAppMaster = LocalClusterContext.get().getMaster().getAppMaster();
      Location masterLoc =
        LocalClusterContext.get().getMaster().getAppMaster().getAppContext().getMasterService()
          .getLocation();
      TConnection connection = TConnectionManager.getConnection(angelAppMaster.getConfig());
      MasterProtocol master = connection.getMasterService(masterLoc.getIp(), masterLoc.getPort());
      int task0Iteration = 2;
      int task1Iteration = 1;

      int jobIteration = (task0Iteration < task1Iteration) ? task0Iteration : task1Iteration;

      master.taskIteration(null, TaskIterationRequest.newBuilder().setIteration(task0Iteration)
        .setTaskId(ProtobufUtil.convertToIdProto(task0Id)).build());
      master.taskIteration(null, TaskIterationRequest.newBuilder().setIteration(task1Iteration)
        .setTaskId(ProtobufUtil.convertToIdProto(task1Id)).build());

      Thread.sleep(1000);

      GetJobReportRequest request =
        GetJobReportRequest.newBuilder().setAppId(LocalClusterContext.get().getAppId().toString())
          .build();

      GetJobReportResponse response = master.getJobReport(null, request);
      assertEquals(response.getJobReport().getJobState(), JobStateProto.J_RUNNING);
      assertEquals(response.getJobReport().getCurIteration(), jobIteration);

      angelAppMaster.getAppContext().getEventHandler().handle(
        new InternalErrorEvent(angelAppMaster.getAppContext().getApplicationId(), "failed"));

      Thread.sleep(5000);

      response = master.getJobReport(null, request);
      assertEquals(response.getJobReport().getJobState(), JobStateProto.J_FAILED);
      assertEquals(response.getJobReport().getCurIteration(), jobIteration);
      assertEquals(response.getJobReport().getDiagnostics(), "failed");

      //Thread.sleep(5000);
      //response = master.getJobReport(null, request);
      //assertEquals(response.getJobReport().getJobState(), JobStateProto.J_FAILED);
      //assertEquals(response.getJobReport().getCurIteration(), jobIteration);
      //assertEquals(response.getJobReport().getDiagnostics(), "failed");

      Thread.sleep(10000);
      try {
        response = master.getJobReport(null, request);
      } catch (Exception x) {
        response = tryGetResponseFromFile(true);
      }

      assertEquals(response.getJobReport().getJobState(), JobStateProto.J_FAILED);
      assertEquals(response.getJobReport().getCurIteration(), jobIteration);
      assertEquals(response.getJobReport().getDiagnostics(), "failed");
    } catch (Exception x) {
      LOG.error("run testGetJobReport failed ", x);
      throw x;
    }
  }

  private GetJobReportResponse tryGetResponseFromFile(boolean deleteOnExist) throws IOException {
    org.apache.hadoop.fs.Path internalStateFile = angelClient.getInternalStateFile();
    GetJobReportResponse response = null;
    FileSystem fs = internalStateFile.getFileSystem(angelClient.getConf());
    if (fs.exists(internalStateFile)) {
      LOG.info(internalStateFile + " exist, parse app report from it");
      FSDataInputStream in = fs.open(internalStateFile);
      response = GetJobReportResponse.parseFrom(in);

      if (deleteOnExist) {
        fs.delete(internalStateFile.getParent(), true);
      }
    }

    return response;
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

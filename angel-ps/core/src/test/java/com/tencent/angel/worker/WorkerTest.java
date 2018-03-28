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

package com.tencent.angel.worker;

import com.tencent.angel.AngelDeployMode;
import com.tencent.angel.client.AngelClient;
import com.tencent.angel.client.AngelClientFactory;
import com.tencent.angel.common.location.Location;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.conf.MatrixConf;
import com.tencent.angel.localcluster.LocalClusterContext;
import com.tencent.angel.localcluster.LocalMaster;
import com.tencent.angel.localcluster.LocalWorker;
import com.tencent.angel.master.AngelApplicationMaster;
import com.tencent.angel.master.DummyTask;
import com.tencent.angel.master.MasterServiceTest;
import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.protobuf.ProtobufUtil;
import com.tencent.angel.protobuf.generated.MLProtos;
import com.tencent.angel.protobuf.generated.WorkerMasterServiceProtos;
import com.tencent.angel.ps.PSAttemptId;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.psagent.PSAgent;
import com.tencent.angel.psagent.PSAgentAttemptId;
import com.tencent.angel.psagent.client.MasterClient;
import com.tencent.angel.utils.NetUtils;
import com.tencent.angel.utils.UGITools;
import com.tencent.angel.worker.storage.DataBlockManager;
import com.tencent.angel.worker.task.Task;
import com.tencent.angel.worker.task.TaskId;
import com.tencent.angel.worker.task.TaskManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.log4j.PropertyConfigurator;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class WorkerTest {
  private static final Log LOG = LogFactory.getLog(MasterServiceTest.class);
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
  private static LocalWorker localWorker;
  private static Worker worker;
  private static LocalMaster localMaster;
  private static AngelApplicationMaster master;
  private static Configuration conf;

  static {
    PropertyConfigurator.configure("../conf/log4j.properties");
  }

  @BeforeClass
  public static void setup() throws Exception {
    try {
      //set basic configuration keys
      conf = new Configuration();
      conf.setBoolean("mapred.mapper.new-api", true);
      conf.setBoolean(AngelConf.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST, true);
      conf.set(AngelConf.ANGEL_TASK_USER_TASKCLASS, DummyTask.class.getName());

      //use local deploy mode and dummy dataspliter
      conf.set(AngelConf.ANGEL_DEPLOY_MODE, "LOCAL");
      conf.setBoolean(AngelConf.ANGEL_AM_USE_DUMMY_DATASPLITER, true);
      conf.set(AngelConf.ANGEL_INPUTFORMAT_CLASS, CombineTextInputFormat.class.getName());
      conf.set(AngelConf.ANGEL_SAVE_MODEL_PATH, LOCAL_FS + TMP_PATH + "/out");
      conf.set(AngelConf.ANGEL_TRAIN_DATA_PATH, LOCAL_FS + TMP_PATH + "/in");
      conf.set(AngelConf.ANGEL_LOG_PATH, LOCAL_FS + TMP_PATH + "/log");

      conf.setInt(AngelConf.ANGEL_WORKERGROUP_NUMBER, 1);
      conf.setInt(AngelConf.ANGEL_PS_NUMBER, 1);
      conf.setInt(AngelConf.ANGEL_WORKER_TASK_NUMBER, 2);

      //get a angel client
      angelClient = AngelClientFactory.get(conf);

      //add matrix
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

      mMatrix = new MatrixContext();
      mMatrix.setName("w2");
      mMatrix.setRowNum(1);
      mMatrix.setColNum(100000);
      mMatrix.setMaxRowNumInBlock(1);
      mMatrix.setMaxColNumInBlock(50000);
      mMatrix.setRowType(RowType.T_DOUBLE_DENSE);
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
    } catch (Exception x) {
      LOG.error("setup failed ", x);
      throw x;
    }
  }

  @Test
  public void testWorkerTaskManager() throws Exception {
    try {
      LOG.info("===========================testWorkerInitAndStart===============================");
      localWorker = LocalClusterContext.get().getWorker(worker0Attempt0Id);
      worker = localWorker.getWorker();

      //test worker getActiveTaskNum
      assertEquals(2, worker.getActiveTaskNum());

      //test worker getTaskNum
      assertEquals(2, worker.getTaskNum());

      //test worker getTaskManager
      TaskManager taskManager = worker.getTaskManager();
      assertTrue(taskManager != null);
      assertEquals(2, taskManager.getTaskCount());

      Task task_0 = taskManager.getRunningTask().get(task0Id);
      assertTrue( task_0 != null);
      Task task_1 = taskManager.getRunningTask().get(task1Id);
      assertTrue( task_1 != null);

      assertTrue(taskManager.isAllTaskRunning());
    } catch (Exception x) {
      LOG.error("run testWorkerTaskManager failed ", x);
      throw x;
    }
  }

  @Test
  public void testWorker() throws Exception {
    try {
      localWorker = LocalClusterContext.get().getWorker(worker0Attempt0Id);
      worker = localWorker.getWorker();
      assertTrue(worker != null);

      //test workerId
      assertEquals(worker0Id, worker.getWorkerId());

      WorkerId wid = new WorkerId("Worker_0_0");
      assertEquals(wid, worker0Id);
      assertTrue(worker0Id.equals(wid));

      //test workerAttemptId
      assertEquals(worker0Attempt0Id, worker.getWorkerAttemptId());

      WorkerAttemptId waId = new WorkerAttemptId("WorkerAttempt_0_0_0");
      assertEquals(worker0Attempt0Id, waId);
      assertTrue(worker0Attempt0Id.equals(waId));

      assertEquals(ProtobufUtil.convertToIdProto(worker0Attempt0Id), worker.getWorkerAttemptIdProto());

      //tet worker initFinished
      assertTrue(worker.isWorkerInitFinished());

      //test worker getInitMinclock
      assertEquals(0, worker.getInitMinClock());

      //test worker loacation
      Location location = worker.getWorkerService().getLocation();
      String localIp = NetUtils.getRealLocalIP();
      assertEquals(localIp, location.getIp());
      int port = location.getPort();
      assertTrue(port >  0 && port < 655355);
    } catch (Exception x) {
      LOG.error("run testWorker failed ", x);
      throw x;
    }
  }

  @Test
  public void testApplicaiotnInfo() throws Exception {
    try {
      localWorker = LocalClusterContext.get().getWorker(worker0Attempt0Id);
      worker = localWorker.getWorker();

      //test AppId
      assertEquals(LocalClusterContext.get().getAppId(), worker.getAppId());
      //test Conf
      assertEquals(conf, worker.getConf());
      //test UserName
      assertEquals(UGITools.getCurrentUser(conf).getShortUserName(), worker.getUser());
    } catch (Exception x) {
      LOG.error("run testApplicaiotnInfo failed ", x);
      throw x;
    }
  }

  @Test
  public void testMaster() throws Exception {
    try {
      localWorker = LocalClusterContext.get().getWorker(worker0Attempt0Id);
      worker = localWorker.getWorker();
      localMaster = LocalClusterContext.get().getMaster();
      master = localMaster.getAppMaster();
      assertTrue(master != null);

      //master location
      Location masterLoc =
        LocalClusterContext.get().getMaster().getAppMaster().getAppContext().getMasterService()
          .getLocation();
      assertEquals(masterLoc ,worker.getMasterLocation());

      //masterClient
      MasterClient masterClient = worker.getPSAgent().getMasterClient();
      WorkerMasterServiceProtos.WorkerRegisterResponse response = masterClient.workerRegister();
      assertTrue(response != null);
      assertEquals(WorkerMasterServiceProtos.WorkerCommandProto.W_SUCCESS, response.getCommand());
    } catch (Exception x) {
      LOG.error("run testMaster failed ", x);
      throw x;
    }
  }

  @Test
  public void testPsAgent() throws Exception {
    try {
      localWorker = LocalClusterContext.get().getWorker(worker0Attempt0Id);
      worker = localWorker.getWorker();

      //test psAgent
      PSAgent psAgent = worker.getPSAgent();
      assertTrue(psAgent != null);

      int psAgentId = psAgent.getId();
      Assert.assertEquals(psAgentId, 1);

      assertEquals(psAgent.getMasterLocation(), worker.getMasterLocation());
    } catch (Exception x) {
      LOG.error("run testPsAgent failed ", x);
      throw x;
    }
  }

  @Test
  public void testDdataBlocker() throws Exception {
    try {
      localWorker = LocalClusterContext.get().getWorker(worker0Attempt0Id);
      worker = localWorker.getWorker();

      //test worker get dataBlockManager
      DataBlockManager dataBlockManager = worker.getDataBlockManager();
      assertTrue(dataBlockManager != null);
      assertEquals(dataBlockManager.getSplitClassification(), worker.getWorkerGroup().getSplits());
    } catch (Exception x) {
      LOG.error("run testDdataBlocker failed ", x);
      throw x;
    }
  }

  @Test
  public void testWorkerGroup() throws Exception {
    try {
      LOG.info("===========================testWorkerGroup===============================");
      worker = LocalClusterContext.get().getWorker(worker0Attempt0Id).getWorker();
      WorkerGroup workerGroup = worker.getWorkerGroup();
      assertTrue(workerGroup != null);

      //workerGroup.getSplits();
      assertEquals(group0Id, workerGroup.getWorkerGroupId());
      WorkerGroupId gid = new WorkerGroupId("WorkerGroup_0");
      assertEquals(gid, workerGroup.getWorkerGroupId());

      Map<WorkerId, WorkerRef> workerMap = workerGroup.getWorkerMap();
      assertTrue(workerMap != null);
      assertEquals(1, workerMap.size());
      WorkerRef workerRef =workerMap.get(worker0Attempt0Id);
    } catch (Exception x) {
      LOG.error("run testWorkerGroup failed ", x);
      throw x;
    }
  }

  @Test
  public void testWorkerContext() throws IOException {
    try {
      localWorker = LocalClusterContext.get().getWorker(worker0Attempt0Id);
      worker = localWorker.getWorker();

      WorkerContext context = WorkerContext.get();
      assertTrue(context != null);

      //application
      ApplicationId appid = context.getAppId();
      assertTrue(appid != null);
      assertEquals(LocalClusterContext.get().getAppId(), appid);
      assertEquals(worker.getUser(), context.getUser());
      assertEquals(AngelDeployMode.LOCAL, context.getDeployMode());
      assertEquals(conf, context.getConf());
      assertEquals(0, context.getInitMinClock());

      //lcation
      String localIp = NetUtils.getRealLocalIP();
      Location location = context.getLocation();
      assertEquals(localIp, location.getIp());
      int port = location.getPort();
      assertTrue(port >  0 && port < 655355);

      //workerGroup info
      assertEquals(group0Id, context.getWorkerGroupId());

      //worker info
      Worker w = context.getWorker();
      assertTrue(w != null);
      assertTrue(w.equals(worker));

      WorkerId wid = context.getWorkerId();
      assertEquals(worker0Id, wid);
      assertEquals(worker0Attempt0Id, context.getWorkerAttemptId());
      assertEquals(ProtobufUtil.convertToIdProto(worker0Attempt0Id), context.getWorkerAttemptIdProto());

      Map<String, String> workerMetrics = context.getWorkerMetrics();
      assertTrue(workerMetrics != null);

      assertEquals(worker, context.getWorker());
      assertEquals(worker.getDataBlockManager(), context.getDataBlockManager());
      assertEquals(worker.getPSAgent(), context.getPSAgent());

      //task
      assertEquals(2, context.getActiveTaskNum());
      assertEquals(worker.getTaskManager(), context.getTaskManager());
    } catch (Exception x) {
      LOG.error("run testWorkerContext failed ", x);
      throw x;
    }
  }

  @AfterClass
  public static void stop() throws IOException{
    try {
      LOG.info("stop local cluster");
      angelClient.stop();
    } catch (Exception x) {
      LOG.error("stop failed ", x);
      throw x;
    }
  }
}

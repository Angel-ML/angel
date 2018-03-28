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

package com.tencent.angel.psagent;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.client.AngelClient;
import com.tencent.angel.client.AngelClientFactory;
import com.tencent.angel.common.location.Location;
import com.tencent.angel.common.location.LocationManager;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.conf.MatrixConf;
import com.tencent.angel.ipc.TConnection;
import com.tencent.angel.localcluster.LocalClusterContext;
import com.tencent.angel.master.AngelApplicationMaster;
import com.tencent.angel.master.DummyTask;
import com.tencent.angel.master.task.AMTaskManager;
import com.tencent.angel.master.worker.WorkerManager;
import com.tencent.angel.ml.math.TVector;
import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.MatrixMetaManager;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ps.PSAttemptId;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.psagent.client.MasterClient;
import com.tencent.angel.psagent.clock.ClockCache;
import com.tencent.angel.psagent.consistency.ConsistencyController;
import com.tencent.angel.psagent.matrix.PSAgentLocationManager;
import com.tencent.angel.psagent.matrix.PSAgentMatrixMetaManager;
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
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.log4j.PropertyConfigurator;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

//import com.tencent.angel.psagent.consistency.SSPConsistencyController;

// @RunWith(MockitoJUnitRunner.class)
public class PSAgentTest {

  private static final Log LOG = LogFactory.getLog(PSAgentTest.class);

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

  static {
    PropertyConfigurator.configure("../conf/log4j.properties");
  }

  @BeforeClass
  public static void setup() throws Exception {
    try {
      // set basic configuration keys
      Configuration conf = new Configuration();
      conf.setBoolean("mapred.mapper.new-api", true);
      conf.setBoolean(AngelConf.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST, true);
      conf.set(AngelConf.ANGEL_TASK_USER_TASKCLASS, DummyTask.class.getName());

      // use local deploy mode and dummy dataspliter
      conf.set(AngelConf.ANGEL_DEPLOY_MODE, "LOCAL");
      conf.setBoolean(AngelConf.ANGEL_AM_USE_DUMMY_DATASPLITER, true);
      // conf.setInt(AngelConf.ANGEL_PREPROCESS_VECTOR_MAXDIM, 10000);
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
      mMatrix.setRowType(RowType.T_DOUBLE_DENSE);
      mMatrix.set(MatrixConf.MATRIX_OPLOG_ENABLEFILTER, "false");
      mMatrix.set(MatrixConf.MATRIX_HOGWILD, "true");
      mMatrix.set(MatrixConf.MATRIX_AVERAGE, "false");
      mMatrix.set(MatrixConf.MATRIX_OPLOG_TYPE, "DENSE_DOUBLE");
      angelClient.addMatrix(mMatrix);

      MatrixContext mMatrix2 = new MatrixContext();
      mMatrix2.setName("w2");
      mMatrix2.setRowNum(1);
      mMatrix2.setColNum(100000);
      mMatrix2.setMaxRowNumInBlock(1);
      mMatrix2.setMaxColNumInBlock(50000);
      mMatrix2.setRowType(RowType.T_DOUBLE_DENSE);
      mMatrix2.set(MatrixConf.MATRIX_OPLOG_ENABLEFILTER, "false");
      mMatrix2.set(MatrixConf.MATRIX_HOGWILD, "true");
      mMatrix2.set(MatrixConf.MATRIX_AVERAGE, "false");
      mMatrix2.set(MatrixConf.MATRIX_OPLOG_TYPE, "DENSE_DOUBLE");
      angelClient.addMatrix(mMatrix2);

      angelClient.startPSServer();
      angelClient.run();
      Thread.sleep(10000);
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
  public void testMasterClient() throws Exception {
    try {
      AngelApplicationMaster angelAppMaster = LocalClusterContext.get().getMaster().getAppMaster();
      assertTrue(angelAppMaster != null);

      AMTaskManager taskManager = angelAppMaster.getAppContext().getTaskManager();
      assertTrue(taskManager != null);

      WorkerManager workerManager = angelAppMaster.getAppContext().getWorkerManager();
      assertTrue(workerManager != null);

      Worker worker = LocalClusterContext.get().getWorker(worker0Attempt0Id).getWorker();

      PSAgentMatrixMetaManager matrixMetaManager = worker.getPSAgent().getMatrixMetaManager();
      int w1Id = matrixMetaManager.getMatrixId("w1");
      int w2Id = matrixMetaManager.getMatrixId("w2");

      MasterClient masterClient = worker.getPSAgent().getMasterClient();
      assertTrue(masterClient != null);

      Location location = masterClient.getPSLocation(psId);
      String ipRegex =
        "(2[5][0-5]|2[0-4]\\d|1\\d{2}|\\d{1,2})\\.(25[0-5]|2[0-4]\\d|1\\d{2}|\\d{1,2})\\.(25[0-5]|2[0-4]\\d|1\\d{2}|\\d{1,2})\\.(25[0-5]|2[0-4]\\d|1\\d{2}|\\d{1,2})";
      Pattern pattern = Pattern.compile(ipRegex);
      Matcher matcher = pattern.matcher(location.getIp());
      assertTrue(matcher.matches());
      assertTrue(location.getPort() >= 1 && location.getPort() <= 65535);

      Map<ParameterServerId, Location> psLocations = masterClient.getPSLocations();
      assertEquals(psLocations.size(), 1);
    } catch (Exception x) {
      LOG.error("run testMasterClient failed ", x);
      throw x;
    }
  }

  @Test
  public void testPSClient() throws Exception {
    try {
      AngelApplicationMaster angelAppMaster = LocalClusterContext.get().getMaster().getAppMaster();
      assertTrue(angelAppMaster != null);

      AMTaskManager taskManager = angelAppMaster.getAppContext().getTaskManager();
      assertTrue(taskManager != null);

      WorkerManager workerManager = angelAppMaster.getAppContext().getWorkerManager();
      assertTrue(workerManager != null);

      Worker worker = LocalClusterContext.get().getWorker(worker0Attempt0Id).getWorker();
      assertTrue(worker != null);

      PSAgent psAgent = worker.getPSAgent();
      assertTrue(psAgent != null);

      // psAgent.initAndStart();

      // test conf
      Configuration conf = psAgent.getConf();
      assertTrue(conf != null);
      assertEquals(conf.get(AngelConf.ANGEL_DEPLOY_MODE), "LOCAL");

      // test master location
      Location masterLoc = psAgent.getMasterLocation();
      String ipRegex =
        "(2[5][0-5]|2[0-4]\\d|1\\d{2}|\\d{1,2})\\.(25[0-5]|2[0-4]\\d|1\\d{2}|\\d{1,2})\\.(25[0-5]|2[0-4]\\d|1\\d{2}|\\d{1,2})\\.(25[0-5]|2[0-4]\\d|1\\d{2}|\\d{1,2})";
      Pattern pattern = Pattern.compile(ipRegex);
      Matcher matcher = pattern.matcher(masterLoc.getIp());
      assertTrue(matcher.matches());
      assertTrue(masterLoc.getPort() >= 1 && masterLoc.getPort() <= 65535);

      // test app id
      ApplicationId appId = psAgent.getAppId();

      // test user
      String user = psAgent.getUser();

      // test ps agent attempt id
      int psAgentId = psAgent.getId();
      assertEquals(psAgentId, 1);

      // test connection
      TConnection conn = psAgent.getConnection();
      assertTrue(conn != null);

      // test master client
      MasterClient masterClient = psAgent.getMasterClient();
      assertTrue(masterClient != null);

      // test ip
      String ip = psAgent.getIp();
      matcher = pattern.matcher(ip);
      assertTrue(matcher.matches());

      // test loc
      Location loc = psAgent.getLocation();
      assertTrue(loc != null);
      matcher = pattern.matcher(loc.getIp());
      assertTrue(matcher.matches());
      assertTrue(loc.getPort() >= 1 && loc.getPort() <= 65535);
    } catch (Exception x) {
      LOG.error("run testPSClient failed ", x);
      throw  x;
    }
  }

  @Test
  public void testLocationCache() throws Exception {
    try {
      AngelApplicationMaster angelAppMaster = LocalClusterContext.get().getMaster().getAppMaster();
      assertTrue(angelAppMaster != null);

      AMTaskManager taskManager = angelAppMaster.getAppContext().getTaskManager();
      assertTrue(taskManager != null);

      WorkerManager workerManager = angelAppMaster.getAppContext().getWorkerManager();
      assertTrue(workerManager != null);

      Worker worker = LocalClusterContext.get().getWorker(worker0Attempt0Id).getWorker();

      PSAgent psAgent = worker.getPSAgent();
      assertTrue(psAgent != null);

      PSAgentLocationManager locationCache = psAgent.getLocationManager();
      assertTrue(locationCache != null);

      // test master location
      Location masterLoc = locationCache.getMasterLocation();
      String ipRegex =
        "(2[5][0-5]|2[0-4]\\d|1\\d{2}|\\d{1,2})\\.(25[0-5]|2[0-4]\\d|1\\d{2}|\\d{1,2})\\.(25[0-5]|2[0-4]\\d|1\\d{2}|\\d{1,2})\\.(25[0-5]|2[0-4]\\d|1\\d{2}|\\d{1,2})";
      Pattern pattern = Pattern.compile(ipRegex);
      Matcher matcher = pattern.matcher(masterLoc.getIp());
      assertTrue(matcher.matches());
      assertTrue(masterLoc.getPort() >= 1 && masterLoc.getPort() <= 65535);

      // test ps location
      Location psLoc = locationCache.getPsLocation(psId);
      matcher = pattern.matcher(psLoc.getIp());
      assertTrue(matcher.matches());
      assertTrue(psLoc.getPort() >= 1 && psLoc.getPort() <= 65535);
      // assertEquals(psLoc, locationCache.updateAndGetPSLocation(psId));

      // test all ps ids
      ParameterServerId[] allPSIds = locationCache.getPsIds();
      assertEquals(allPSIds.length, 1);
      assertEquals(allPSIds[0], psId);
    } catch (Exception x) {
      LOG.error("run testLocationCache failed ", x);
      throw x;
    }
  }

  @Test
  public void testMatrixMetaManager() throws Exception {
    try {
      AngelApplicationMaster angelAppMaster = LocalClusterContext.get().getMaster().getAppMaster();
      assertTrue(angelAppMaster != null);

      AMTaskManager taskManager = angelAppMaster.getAppContext().getTaskManager();
      assertTrue(taskManager != null);

      WorkerManager workerManager = angelAppMaster.getAppContext().getWorkerManager();
      assertTrue(workerManager != null);

      Worker worker = LocalClusterContext.get().getWorker(worker0Attempt0Id).getWorker();
      assertTrue(worker != null);

      PSAgent psAgent = worker.getPSAgent();
      assertTrue(psAgent != null);

      PSAgentMatrixMetaManager matrixMetaManager = psAgent.getMatrixMetaManager();
      assertTrue(matrixMetaManager != null);

      // test all matrix ids
      assertEquals(matrixMetaManager.getMatrixMetas().size(), 2);

      // test all matrix names
      assertNotNull(matrixMetaManager.getMatrixMeta("w1"));
      assertNotNull(matrixMetaManager.getMatrixMeta("w2"));

      // test matrix attribute
      int matrixId1 = matrixMetaManager.getMatrixId("w1");
      int matrixId2 = matrixMetaManager.getMatrixId("w2");
      String hogwildAttr =
        matrixMetaManager.getMatrixMeta(matrixId1).getAttribute(MatrixConf.MATRIX_HOGWILD, "true");
      assertEquals(hogwildAttr, "true");
      hogwildAttr =
        matrixMetaManager.getMatrixMeta(matrixId2).getAttribute(MatrixConf.MATRIX_HOGWILD, "true");
      assertEquals(hogwildAttr, "true");

      int matrix1Id = LocalClusterContext.get().getMaster().getAppMaster().getAppContext().getMatrixMetaManager().getMatrix("w1").getId();
      int matrix2Id = LocalClusterContext.get().getMaster().getAppMaster().getAppContext().getMatrixMetaManager().getMatrix("w2").getId();

      // test matrix meta
      MatrixMeta matrixMetaById = matrixMetaManager.getMatrixMeta(matrix1Id);
      MatrixMeta matrixMetaByName = matrixMetaManager.getMatrixMeta("w1");
      assertEquals(matrixMetaById, matrixMetaByName);
      assertEquals(matrixMetaById.getName(), "w1");
      assertEquals(matrixMetaById.getRowNum(), 1);
      assertEquals(matrixMetaById.getColNum(), 100000);
      assertEquals(matrixMetaById.getRowType(), RowType.T_DOUBLE_DENSE);
      assertEquals(matrixMetaById.getAttribute(MatrixConf.MATRIX_HOGWILD, "true"), "true");
      assertEquals(matrixMetaById.getStaleness(), 0);
    } catch (Exception x) {
      LOG.error("run testMatrixMetaManager failed ", x);
      throw x;
    }
  }

  @Test
  public void testMatrixLocationManager() throws Exception {
    try {
      AngelApplicationMaster angelAppMaster = LocalClusterContext.get().getMaster().getAppMaster();
      assertTrue(angelAppMaster != null);

      AMTaskManager taskManager = angelAppMaster.getAppContext().getTaskManager();
      assertTrue(taskManager != null);

      WorkerManager workerManager = angelAppMaster.getAppContext().getWorkerManager();
      assertTrue(workerManager != null);

      Worker worker = LocalClusterContext.get().getWorker(worker0Attempt0Id).getWorker();
      assertTrue(worker != null);

      PSAgent psAgent = worker.getPSAgent();
      assertTrue(psAgent != null);

      PSAgentMatrixMetaManager matrixPartitionRouter = psAgent.getMatrixMetaManager();
      PSAgentLocationManager locationCache = psAgent.getLocationManager();
      assertTrue(matrixPartitionRouter != null);

      // test ps location
      Location psLoc = locationCache.getPsLocation(psId);
      String ipRegex =
        "(2[5][0-5]|2[0-4]\\d|1\\d{2}|\\d{1,2})\\.(25[0-5]|2[0-4]\\d|1\\d{2}|\\d{1,2})\\.(25[0-5]|2[0-4]\\d|1\\d{2}|\\d{1,2})\\.(25[0-5]|2[0-4]\\d|1\\d{2}|\\d{1,2})";
      Pattern pattern = Pattern.compile(ipRegex);
      Matcher matcher = pattern.matcher(psLoc.getIp());
      assertTrue(matcher.matches());
      assertTrue(psLoc.getPort() >= 1 && psLoc.getPort() <= 65535);

      int matrix1Id = LocalClusterContext.get().getMaster().getAppMaster().getAppContext().getMatrixMetaManager().getMatrix("w1").getId();
      int matrix2Id = LocalClusterContext.get().getMaster().getAppMaster().getAppContext().getMatrixMetaManager().getMatrix("w2").getId();

      // test partitions
      List<PartitionKey> partition1Keys = matrixPartitionRouter.getPartitions(matrix1Id);
      assertEquals(partition1Keys.size(), 2);
      List<PartitionKey> partition2Keys = matrixPartitionRouter.getPartitions(matrix1Id);
      assertEquals(partition2Keys.size(), 2);

      partition1Keys.clear();
      partition1Keys = matrixPartitionRouter.getPartitions(matrix1Id, 0);
      assertEquals(partition1Keys.size(), 2);
      partition2Keys.clear();
      partition2Keys = matrixPartitionRouter.getPartitions(matrix1Id, 0);
      assertEquals(partition2Keys.size(), 2);

      int rowPartSize = matrixPartitionRouter.getRowPartitionSize(matrix1Id, 0);
      assertEquals(rowPartSize, 2);
      rowPartSize = matrixPartitionRouter.getRowPartitionSize(matrix1Id, 0);
      assertEquals(rowPartSize, 2);
    } catch (Exception x) {
      LOG.error("run testMatrixLocationManager failed ", x);
      throw x;
    }
  }

  @Test
  public void testPSAgentContext() throws Exception {
    try {
      AngelApplicationMaster angelAppMaster = LocalClusterContext.get().getMaster().getAppMaster();
      assertTrue(angelAppMaster != null);

      AMTaskManager taskManager = angelAppMaster.getAppContext().getTaskManager();
      assertTrue(taskManager != null);

      WorkerManager workerManager = angelAppMaster.getAppContext().getWorkerManager();
      assertTrue(workerManager != null);

      Worker worker = LocalClusterContext.get().getWorker(worker0Attempt0Id).getWorker();
      assertTrue(worker != null);

      PSAgent psAgent = worker.getPSAgent();
      assertTrue(psAgent != null);

      PSAgentContext psAgentContext = PSAgentContext.get();
      assertTrue(psAgentContext.getPsAgent() != null);
      assertTrue(psAgentContext.getConf() != null);
      assertTrue(psAgentContext.getMetrics() != null);
      assertTrue(psAgentContext.getMasterClient() != null);
      assertTrue(psAgentContext.getOpLogCache() != null);
      assertTrue(psAgentContext.getMatrixTransportClient() != null);
      assertTrue(psAgentContext.getMatrixMetaManager() != null);
      assertTrue(psAgentContext.getMatrixMetaManager() != null);
      assertTrue(psAgentContext.getLocationManager() != null);

      assertEquals(psAgentContext.getRunningMode(), psAgent.getRunningMode());
      assertEquals(psAgentContext.getIp(), psAgent.getIp());
      assertEquals(
        psAgentContext.getStaleness(),
        psAgent.getConf().getInt(AngelConf.ANGEL_STALENESS,
          AngelConf.DEFAULT_ANGEL_STALENESS));
      assertEquals(psAgentContext.getConsistencyController(), psAgent.getConsistencyController());
      assertEquals(psAgentContext.getMatrixOpLogCache(), psAgent.getOpLogCache());
      assertEquals(psAgentContext.getClockCache(), psAgent.getClockCache());
      assertEquals(psAgentContext.getMatricesCache(), psAgent.getMatricesCache());
      assertEquals(psAgentContext.getMatrixStorageManager(), psAgent.getMatrixStorageManager());
      assertEquals(psAgentContext.getMatrixClientAdapter(), psAgent.getMatrixClientAdapter());
      assertEquals(psAgentContext.getExecutor(), psAgent.getExecutor());

      assertTrue(psAgentContext.getTaskContext(1) != null);
      assertTrue(psAgentContext.getTaskContext(2) != null);


      int taskNum = psAgentContext.getTotalTaskNum();
      assertEquals(taskNum, 2);

      int localTaskNum = psAgentContext.getLocalTaskNum();
      assertEquals(localTaskNum, 2);
    } catch (Exception x) {
      LOG.error("run testPSAgentContext failed ", x);
      throw x;
    }
  }

  @Test
  public void testTaskContext() throws Exception {
    try {
      AngelApplicationMaster angelAppMaster = LocalClusterContext.get().getMaster().getAppMaster();
      assertTrue(angelAppMaster != null);

      AMTaskManager taskManager = angelAppMaster.getAppContext().getTaskManager();
      assertTrue(taskManager != null);

      WorkerManager workerManager = angelAppMaster.getAppContext().getWorkerManager();
      assertTrue(workerManager != null);

      Worker worker = LocalClusterContext.get().getWorker(worker0Attempt0Id).getWorker();
      assertTrue(worker != null);

      PSAgent psAgent = worker.getPSAgent();
      assertTrue(psAgent != null);

      PSAgentContext psAgentContext = PSAgentContext.get();
      assertTrue(psAgentContext.getPsAgent() != null);

      TaskContext taskContext1 = psAgentContext.getTaskContext(1);
      TaskContext taskContext2 = psAgentContext.getTaskContext(2);
      assertTrue(taskContext1 != null);
      assertTrue(taskContext2 != null);

      assertEquals(taskContext1.getIndex(), 1);
      assertEquals(taskContext2.getIndex(), 2);

      assertEquals(taskContext1.getEpoch(), 0);
      assertEquals(taskContext2.getEpoch(), 0);

      assertEquals(taskContext1.getMatrixClock(1), 0);
      assertEquals(taskContext2.getMatrixClock(2), 0);

      assertEquals(taskContext1.getMatrixClocks().size(), 1);
      assertEquals(taskContext2.getMatrixClocks().size(), 1);

      assertEquals(taskContext1.getProgress(), 0.0, 1e-5);
      assertEquals(taskContext2.getProgress(), 0.0, 1e-5);
    } catch (Exception x) {
      LOG.error("run testTaskContext failed ", x);
      throw x;
    }
  }

  @Test
  public void testConsistencyController() throws Exception {
    try {
      AngelApplicationMaster angelAppMaster = LocalClusterContext.get().getMaster().getAppMaster();
      assertTrue(angelAppMaster != null);

      AMTaskManager taskManager = angelAppMaster.getAppContext().getTaskManager();
      assertTrue(taskManager != null);

      WorkerManager workerManager = angelAppMaster.getAppContext().getWorkerManager();
      assertTrue(workerManager != null);

      Worker worker = LocalClusterContext.get().getWorker(worker0Attempt0Id).getWorker();
      assertTrue(worker != null);

      PSAgent psAgent = worker.getPSAgent();
      assertTrue(psAgent != null);

      ConsistencyController consistControl = psAgent.getConsistencyController();
      assertTrue(consistControl != null);

      PSAgentContext psAgentContext = PSAgentContext.get();
      assertTrue(psAgentContext.getPsAgent() != null);

      TaskContext taskContext1 = psAgentContext.getTaskContext(1);
      TaskContext taskContext2 = psAgentContext.getTaskContext(2);
      assertTrue(taskContext1 != null);
      assertTrue(taskContext2 != null);

      int matrix1Id = LocalClusterContext.get().getMaster().getAppMaster().getAppContext().getMatrixMetaManager().getMatrix("w1").getId();
      int matrix2Id = LocalClusterContext.get().getMaster().getAppMaster().getAppContext().getMatrixMetaManager().getMatrix("w2").getId();

      TVector row1 = consistControl.getRow(taskContext1, matrix1Id, 0);
      assertTrue(row1 != null);
      assertEquals(row1.size(), 100000);

      TVector row2 = consistControl.getRow(taskContext1, matrix2Id, 0);
      assertTrue(row2 != null);
      assertEquals(row2.size(), 100000);

      consistControl.clock(taskContext1, matrix1Id, true);
      assertEquals(taskContext1.getMatrixClock(matrix1Id), 1);

      int staleness =
        psAgent.getConf().getInt(AngelConf.ANGEL_STALENESS,
          AngelConf.DEFAULT_ANGEL_STALENESS);
    } catch (Exception x) {
      LOG.error("run testConsistencyController failed ", x);
      throw x;
    }
  }

  @Test
  public void testClockCache() throws Exception {
    try {
      AngelApplicationMaster angelAppMaster = LocalClusterContext.get().getMaster().getAppMaster();
      assertTrue(angelAppMaster != null);

      AMTaskManager taskManager = angelAppMaster.getAppContext().getTaskManager();
      assertTrue(taskManager != null);

      WorkerManager workerManager = angelAppMaster.getAppContext().getWorkerManager();
      assertTrue(workerManager != null);

      Worker worker = LocalClusterContext.get().getWorker(worker0Attempt0Id).getWorker();
      assertTrue(worker != null);

      PSAgent psAgent = worker.getPSAgent();
      assertTrue(psAgent != null);

      ClockCache clockCache = psAgent.getClockCache();
      assertTrue(clockCache != null);

      int rowClock = clockCache.getClock(1, 0);
      assertEquals(rowClock, 0);
    } catch (Exception x) {
      LOG.error("run testClockCache failed ", x);
      throw x;
    }
  }

  @AfterClass
  public static void stop() throws Exception {
    try {
      LOG.info("stop local cluster");
      angelClient.stop();
    } catch (Exception x) {
      LOG.error("stop failed ", x);
      throw x;
    }
  }
}

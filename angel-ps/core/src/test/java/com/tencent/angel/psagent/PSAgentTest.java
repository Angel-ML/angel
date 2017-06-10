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

import com.google.protobuf.ServiceException;
import com.tencent.angel.PartitionKey;
import com.tencent.angel.client.AngelClient;
import com.tencent.angel.client.AngelClientFactory;
import com.tencent.angel.common.Location;
import com.tencent.angel.conf.AngelConfiguration;
import com.tencent.angel.conf.MatrixConfiguration;
import com.tencent.angel.exception.TimeOutException;
import com.tencent.angel.ipc.TConnection;
import com.tencent.angel.localcluster.LocalClusterContext;
import com.tencent.angel.master.AngelApplicationMaster;
import com.tencent.angel.master.DummyTask;
import com.tencent.angel.master.task.AMTask;
import com.tencent.angel.master.task.AMTaskManager;
import com.tencent.angel.master.worker.WorkerManager;
import com.tencent.angel.ml.math.TVector;
import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.MatrixMetaManager;
import com.tencent.angel.protobuf.generated.MLProtos;
import com.tencent.angel.ps.PSAttemptId;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.psagent.client.MasterClient;
import com.tencent.angel.psagent.clock.ClockCache;
import com.tencent.angel.psagent.consistency.ConsistencyController;
//import com.tencent.angel.psagent.consistency.SSPConsistencyController;
import com.tencent.angel.psagent.task.TaskContext;
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
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.log4j.PropertyConfigurator;
import org.junit.*;

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
    // conf.setInt(AngelConfiguration.ANGEL_PREPROCESS_VECTOR_MAXDIM, 10000);
    conf.set(AngelConfiguration.ANGEL_INPUTFORMAT_CLASS, CombineTextInputFormat.class.getName());
    conf.set(AngelConfiguration.ANGEL_SAVE_MODEL_PATH, LOCAL_FS + TMP_PATH + "/out");
    conf.set(AngelConfiguration.ANGEL_TRAIN_DATA_PATH, LOCAL_FS + TMP_PATH + "/in");

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
    mMatrix.setRowType(MLProtos.RowType.T_DOUBLE_DENSE);
    mMatrix.set(MatrixConfiguration.MATRIX_OPLOG_ENABLEFILTER, "false");
    mMatrix.set(MatrixConfiguration.MATRIX_HOGWILD, "true");
    mMatrix.set(MatrixConfiguration.MATRIX_AVERAGE, "false");
    mMatrix.set(MatrixConfiguration.MATRIX_OPLOG_TYPE, "DENSE_DOUBLE");
    angelClient.addMatrix(mMatrix);

    mMatrix.setName("w2");
    mMatrix.setRowNum(1);
    mMatrix.setColNum(100000);
    mMatrix.setMaxRowNumInBlock(1);
    mMatrix.setMaxColNumInBlock(50000);
    mMatrix.setRowType(MLProtos.RowType.T_DOUBLE_DENSE);
    mMatrix.set(MatrixConfiguration.MATRIX_OPLOG_ENABLEFILTER, "false");
    mMatrix.set(MatrixConfiguration.MATRIX_HOGWILD, "true");
    mMatrix.set(MatrixConfiguration.MATRIX_AVERAGE, "false");
    mMatrix.set(MatrixConfiguration.MATRIX_OPLOG_TYPE, "DENSE_DOUBLE");
    angelClient.addMatrix(mMatrix);

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
  }

  @Test
  public void testMasterClient() throws ServiceException, TimeOutException, InterruptedException,
      IOException {
    AngelApplicationMaster angelAppMaster = LocalClusterContext.get().getMaster().getAppMaster();
    assertTrue(angelAppMaster != null);

    AMTaskManager taskManager = angelAppMaster.getAppContext().getTaskManager();
    assertTrue(taskManager != null);

    WorkerManager workerManager = angelAppMaster.getAppContext().getWorkerManager();
    assertTrue(workerManager != null);

    Worker worker = LocalClusterContext.get().getWorker(worker0Attempt0Id).getWorker();

    MatrixMetaManager matrixMetaManager = worker.getPSAgent().getMatrixMetaManager();
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

    // test create matrix
    MatrixContext newMatrix = new MatrixContext();
    newMatrix.setName("new");
    newMatrix.setRowNum(1);
    newMatrix.setColNum(100000);
    newMatrix.setMaxRowNumInBlock(1);
    newMatrix.setMaxColNumInBlock(50000);
    newMatrix.setRowType(MLProtos.RowType.T_DOUBLE_DENSE);
    newMatrix.set(MatrixConfiguration.MATRIX_OPLOG_ENABLEFILTER, "false");
    newMatrix.set(MatrixConfiguration.MATRIX_HOGWILD, "true");
    newMatrix.set(MatrixConfiguration.MATRIX_AVERAGE, "false");
    newMatrix.set(MatrixConfiguration.MATRIX_OPLOG_TYPE, "DENSE_DOUBLE");

    MatrixMeta newMatrixMeta = masterClient.createMatrix(newMatrix, 10000);
    assertEquals(newMatrixMeta.getName(), "new");
    assertEquals(newMatrixMeta.getRowNum(), 1);
    assertEquals(newMatrixMeta.getColNum(), 100000);

    AMTask task0 = taskManager.getTask(task0Id);
    AMTask task1 = taskManager.getTask(task1Id);
    masterClient.updateClock(task0Id.getIndex(), w1Id, 1);
    masterClient.updateClock(task0Id.getIndex(), w2Id, 1);

    Int2IntOpenHashMap matrix0Clocks = task0.getMatrixClocks();
    assertEquals(matrix0Clocks.size(), 2);
    assertEquals(matrix0Clocks.get(w1Id), 1);
    assertEquals(matrix0Clocks.get(w2Id), 1);

    // Int2IntOpenHashMap matrix1Clocks = task1.getMatrixClocks();
    // assertEquals(matrix1Clocks.size(), 2);
    // assertEquals(matrix1Clocks.get(w1Id), 1);
    // assertEquals(matrix1Clocks.get(w2Id), 1);
  }

  @Test
  public void testPSClient() throws Exception {
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
    assertEquals(conf.get(AngelConfiguration.ANGEL_DEPLOY_MODE), "LOCAL");

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
    PSAgentAttemptId psAgentAttemptId = psAgent.getId();
    assertEquals(psAgentAttemptId.toString(), "PSAgentAttempt_0_0");
    assertEquals(psAgentAttemptId.getIndex(), 0);

    // test ps agent id
    PSAgentId psAgentId = psAgentAttemptId.getPsAgentId();
    assertEquals(psAgentId.toString(), "PSAgent_0");
    assertEquals(psAgentId.getIndex(), 0);

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
  }

  @Test
  public void testLocationCache() throws Exception {

    AngelApplicationMaster angelAppMaster = LocalClusterContext.get().getMaster().getAppMaster();
    assertTrue(angelAppMaster != null);

    AMTaskManager taskManager = angelAppMaster.getAppContext().getTaskManager();
    assertTrue(taskManager != null);

    WorkerManager workerManager = angelAppMaster.getAppContext().getWorkerManager();
    assertTrue(workerManager != null);

    Worker worker = LocalClusterContext.get().getWorker(worker0Attempt0Id).getWorker();

    PSAgent psAgent = worker.getPSAgent();
    assertTrue(psAgent != null);

    psAgent.initAndStart();

    LocationCache locationCache = psAgent.getLocationCache();
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
    Location psLoc = locationCache.getPSLocation(psId);
    matcher = pattern.matcher(psLoc.getIp());
    assertTrue(matcher.matches());
    assertTrue(psLoc.getPort() >= 1 && psLoc.getPort() <= 65535);
    // assertEquals(psLoc, locationCache.updateAndGetPSLocation(psId));

    // test all ps ids
    ParameterServerId[] allPSIds = locationCache.getPSIds();
    assertEquals(allPSIds.length, 1);
    assertEquals(allPSIds[0], psId);
  }

  @Test
  public void testMatrixMetaManager() throws Exception {

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

    psAgent.initAndStart();

    MatrixMetaManager matrixMetaManager = psAgent.getMatrixMetaManager();
    assertTrue(matrixMetaManager != null);

    // test all matrix ids
    Set<Integer> matrixIds = matrixMetaManager.getMatrixIds();
    assertEquals(matrixIds.size(), 2);

    // test all matrix names
    Set<String> matrixNames = matrixMetaManager.getMatrixNames();
    assertEquals(matrixNames.size(), 2);
    assertTrue(matrixNames.contains("w1"));
    assertTrue(matrixNames.contains("w1"));


    // test matrix attribute
    int matrixId1 = matrixMetaManager.getMatrixId("w1");
    int matrixId2 = matrixMetaManager.getMatrixId("w2");
    String hogwildAttr =
        matrixMetaManager.getAttribute(matrixId1, MatrixConfiguration.MATRIX_HOGWILD, "true");
    assertEquals(hogwildAttr, "true");
    hogwildAttr =
        matrixMetaManager.getAttribute(matrixId2, MatrixConfiguration.MATRIX_HOGWILD, "true");
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
    assertEquals(matrixMetaById.getRowType(), MLProtos.RowType.T_DOUBLE_DENSE);
    assertEquals(matrixMetaById.getAttribute(MatrixConfiguration.MATRIX_HOGWILD, "true"), "true");
    assertEquals(matrixMetaById.getStaleness(), 0);
  }

  @Test
  public void testMatrixLocationManager() throws Exception {

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

    psAgent.initAndStart();

    MatrixPartitionRouter matrixPartitionRouter = psAgent.getMatrixPartitionRouter();
    LocationCache locationCache = psAgent.getLocationCache();
    assertTrue(matrixPartitionRouter != null);

    // test ps location
    Location psLoc = locationCache.getPSLocation(psId);
    String ipRegex =
    "(2[5][0-5]|2[0-4]\\d|1\\d{2}|\\d{1,2})\\.(25[0-5]|2[0-4]\\d|1\\d{2}|\\d{1,2})\\.(25[0-5]|2[0-4]\\d|1\\d{2}|\\d{1,2})\\.(25[0-5]|2[0-4]\\d|1\\d{2}|\\d{1,2})";
    Pattern pattern = Pattern.compile(ipRegex);
    Matcher matcher = pattern.matcher(psLoc.getIp());
    assertTrue(matcher.matches());
    assertTrue(psLoc.getPort() >= 1 && psLoc.getPort() <= 65535);

    int matrix1Id = LocalClusterContext.get().getMaster().getAppMaster().getAppContext().getMatrixMetaManager().getMatrix("w1").getId();
    int matrix2Id = LocalClusterContext.get().getMaster().getAppMaster().getAppContext().getMatrixMetaManager().getMatrix("w2").getId();

    // test partitions
    List<PartitionKey> partition1Keys = matrixPartitionRouter.getPartitionKeyList(matrix1Id);
    assertEquals(partition1Keys.size(), 2);
    List<PartitionKey> partition2Keys = matrixPartitionRouter.getPartitionKeyList(matrix1Id);
    assertEquals(partition2Keys.size(), 2);

    partition1Keys.clear();
    partition1Keys = matrixPartitionRouter.getPartitionKeyList(matrix1Id, 0);
    assertEquals(partition1Keys.size(), 2);
    partition2Keys.clear();
    partition2Keys = matrixPartitionRouter.getPartitionKeyList(matrix1Id, 0);
    assertEquals(partition2Keys.size(), 2);

    // PartitionKey part1 = matrixPartitionRouter.getPartitionKey(1, 0);
    // assertTrue(part1 != null);
    // assertEquals(part1, partition1Keys.get(0));
    // PartitionKey part2 = matrixPartitionRouter.getPartitionKey(2, 0);
    // assertTrue(part2 != null);
    // assertEquals(part2, partition2Keys.get(1));

    int rowPartSize = matrixPartitionRouter.getRowPartitionSize(matrix1Id, 0);
    assertEquals(rowPartSize, 2);
    rowPartSize = matrixPartitionRouter.getRowPartitionSize(matrix1Id, 0);
    assertEquals(rowPartSize, 2);

    Map<Integer, List<PartitionKey>> allParts = matrixPartitionRouter.getMatrixToPartMap();
    assertEquals(allParts.size(), 2);
  }

  @Test
  public void testPSAgentContext() throws Exception {

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

    psAgent.initAndStart();

    PSAgentContext psAgentContext = PSAgentContext.get();
    assertTrue(psAgentContext.getPsAgent() != null);
    assertTrue(psAgentContext.getConf() != null);
    assertTrue(psAgentContext.getMetrics() != null);
    assertTrue(psAgentContext.getMasterClient() != null);
    assertTrue(psAgentContext.getIdProto() != null);
    assertTrue(psAgentContext.getOpLogCache() != null);
    assertTrue(psAgentContext.getMatrixTransportClient() != null);
    assertTrue(psAgentContext.getMatrixPartitionRouter() != null);
    assertTrue(psAgentContext.getMatrixMetaManager() != null);
    assertTrue(psAgentContext.getLocationCache() != null);

    assertEquals(psAgentContext.getRunningMode(), psAgent.getRunningMode());
    assertEquals(psAgentContext.getIp(), psAgent.getIp());
    assertEquals(
        psAgentContext.getStaleness(),
        psAgent.getConf().getInt(AngelConfiguration.ANGEL_STALENESS,
            AngelConfiguration.DEFAULT_ANGEL_STALENESS));
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

  }

  @Test
  public void testTaskContext() throws Exception {

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

    psAgent.initAndStart();

    PSAgentContext psAgentContext = PSAgentContext.get();
    assertTrue(psAgentContext.getPsAgent() != null);

    TaskContext taskContext1 = psAgentContext.getTaskContext(1);
    TaskContext taskContext2 = psAgentContext.getTaskContext(2);
    assertTrue(taskContext1 != null);
    assertTrue(taskContext2 != null);

    assertEquals(taskContext1.getIndex(), 1);
    assertEquals(taskContext2.getIndex(), 2);

    assertEquals(taskContext1.getIteration(), 0);
    assertEquals(taskContext2.getIteration(), 0);

    assertEquals(taskContext1.getMatrixClock(1), 0);
    assertEquals(taskContext2.getMatrixClock(2), 0);

    assertEquals(taskContext1.getMatrixClocks().size(), 1);
    assertEquals(taskContext2.getMatrixClocks().size(), 1);

    assertEquals(taskContext1.getProgress(), 0.0, 1e-5);
    assertEquals(taskContext2.getProgress(), 0.0, 1e-5);
  }

  @Test
  public void testConsistencyController() throws Exception {

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

    psAgent.initAndStart();

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
        psAgent.getConf().getInt(AngelConfiguration.ANGEL_STALENESS,
            AngelConfiguration.DEFAULT_ANGEL_STALENESS);
    //assertEquals(((SSPConsistencyController) consistControl).getStaleness(), staleness);
  }

  @Test
  public void testClockCache() throws Exception {

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

    psAgent.initAndStart();

    ClockCache clockCache = psAgent.getClockCache();
    assertTrue(clockCache != null);

    int rowClock = clockCache.getClock(1, 0);
    assertEquals(rowClock, 0);

    // PartitionKey part1 = psAgent.getMatrixPartitionRouter().getPartitionKey(1, 0);
    // int part1Clock = clockCache.getClock(1, part1);
    // assertEquals(part1Clock, 0);
    //
    // PartitionKey part2 = psAgent.getMatrixPartitionRouter().getPartitionKey(2, 0);
    // int part2Clock = clockCache.getClock(2, part2);
    // assertEquals(part2Clock, 0);
  }

  @After
  public void stop() throws IOException, InterruptedException {
    LOG.info("stop local cluster");
    angelClient.stop();
    Thread.sleep(10000);
  }
}

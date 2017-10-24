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
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.conf.MatrixConf;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.localcluster.LocalClusterContext;
import com.tencent.angel.master.task.AMTask;
import com.tencent.angel.master.task.AMTaskManager;
import com.tencent.angel.ml.math.vector.DenseIntDoubleVector;
import com.tencent.angel.ml.math.vector.TDoubleVector;
import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.protobuf.ProtobufUtil;
import com.tencent.angel.protobuf.generated.MLProtos;
import com.tencent.angel.protobuf.generated.MLProtos.MatrixPartitionLocation;
import com.tencent.angel.protobuf.generated.MLProtos.MatrixProto;
import com.tencent.angel.protobuf.generated.MLProtos.RowType;
import com.tencent.angel.ps.PSAttemptId;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.ps.impl.MatrixPartitionManager;
import com.tencent.angel.ps.impl.ParameterServer;
import com.tencent.angel.ps.impl.matrix.ServerMatrix;
import com.tencent.angel.ps.impl.matrix.ServerPartition;
import com.tencent.angel.psagent.client.MasterClient;
import com.tencent.angel.psagent.matrix.MatrixClient;
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
import org.apache.log4j.PropertyConfigurator;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class MatrixMetaManagerTest {
  private static final Log LOG = LogFactory.getLog(MatrixMetaManagerTest.class);
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
  public void testMatrixMetaManager() throws  Exception{
    try {
      LOG.info("===========================testMatrixMetaManager===============================");
      AngelApplicationMaster angelAppMaster = LocalClusterContext.get().getMaster().getAppMaster();
      assertTrue(angelAppMaster != null);
      com.tencent.angel.master.MatrixMetaManager matrixMetaManager =
        angelAppMaster.getAppContext().getMatrixMetaManager();
      MatrixProto matrixw1Proto = matrixMetaManager.getMatrix("w1");
      MatrixProto matrixw2Proto = matrixMetaManager.getMatrix("w2");
      assertTrue(matrixw1Proto != null);
      assertTrue(matrixw2Proto != null);

      assertEquals(matrixw1Proto.getRowNum(), 1);
      assertEquals(matrixw1Proto.getColNum(), 100000);
      assertEquals(matrixw1Proto.getMatrixPartLocationCount(), 2);
      List<MatrixPartitionLocation> w1Parts = matrixw1Proto.getMatrixPartLocationList();
      assertEquals(w1Parts.get(0).getPsId(), ProtobufUtil.convertToIdProto(psId));
      assertEquals(w1Parts.get(0).getPart().getPartitionId(), 0);
      assertEquals(w1Parts.get(0).getPart().getStartRow(), 0);
      assertEquals(w1Parts.get(0).getPart().getEndRow(), 1);
      assertEquals(w1Parts.get(0).getPart().getStartCol(), 0);
      assertEquals(w1Parts.get(0).getPart().getEndCol(), 50000);
      assertEquals(w1Parts.get(1).getPart().getPartitionId(), 1);
      assertEquals(w1Parts.get(1).getPart().getStartRow(), 0);
      assertEquals(w1Parts.get(1).getPart().getEndRow(), 1);
      assertEquals(w1Parts.get(1).getPart().getStartCol(), 50000);
      assertEquals(w1Parts.get(1).getPart().getEndCol(), 100000);

      List<MatrixPartitionLocation> w2Parts = matrixw2Proto.getMatrixPartLocationList();
      assertEquals(w2Parts.get(0).getPsId(), ProtobufUtil.convertToIdProto(psId));
      assertEquals(w2Parts.get(0).getPart().getPartitionId(), 0);
      assertEquals(w2Parts.get(0).getPart().getStartRow(), 0);
      assertEquals(w2Parts.get(0).getPart().getEndRow(), 1);
      assertEquals(w2Parts.get(0).getPart().getStartCol(), 0);
      assertEquals(w2Parts.get(0).getPart().getEndCol(), 50000);
      assertEquals(w2Parts.get(1).getPart().getPartitionId(), 1);
      assertEquals(w2Parts.get(1).getPart().getStartRow(), 0);
      assertEquals(w2Parts.get(1).getPart().getEndRow(), 1);
      assertEquals(w2Parts.get(1).getPart().getStartCol(), 50000);
      assertEquals(w2Parts.get(1).getPart().getEndCol(), 100000);
    } catch (Exception x) {
      LOG.error("run testMatrixMetaManager failed ", x);
      throw x;
    }
  }


  @Test
  public void testCreateMatrix() throws Exception {
    try{
      LOG.info("===========================testCreateMatrix===============================");
      Worker worker = LocalClusterContext.get().getWorker(worker0Attempt0Id).getWorker();
      MasterClient masterClient = worker.getPSAgent().getMasterClient();

      int w3Id = -1;
      int w4Id = -1;
      // add matrix
      MatrixContext mMatrix = new MatrixContext();
      mMatrix.setName("w3");
      mMatrix.setRowNum(1);
      mMatrix.setColNum(100000);
      mMatrix.setMaxRowNumInBlock(1);
      mMatrix.setMaxColNumInBlock(50000);
      mMatrix.setRowType(MLProtos.RowType.T_DOUBLE_DENSE);
      mMatrix.set(MatrixConf.MATRIX_OPLOG_ENABLEFILTER, "false");
      mMatrix.set(MatrixConf.MATRIX_HOGWILD, "true");
      mMatrix.set(MatrixConf.MATRIX_AVERAGE, "false");
      mMatrix.set(MatrixConf.MATRIX_OPLOG_TYPE, "DENSE_DOUBLE");
      masterClient.createMatrix(mMatrix, 10000);

      mMatrix.setName("w4");
      mMatrix.setRowNum(1);
      mMatrix.setColNum(100000);
      mMatrix.setMaxRowNumInBlock(1);
      mMatrix.setMaxColNumInBlock(50000);
      mMatrix.setRowType(MLProtos.RowType.T_DOUBLE_DENSE);
      mMatrix.set(MatrixConf.MATRIX_OPLOG_ENABLEFILTER, "false");
      mMatrix.set(MatrixConf.MATRIX_HOGWILD, "true");
      mMatrix.set(MatrixConf.MATRIX_AVERAGE, "false");
      mMatrix.set(MatrixConf.MATRIX_OPLOG_TYPE, "DENSE_DOUBLE");
      masterClient.createMatrix(mMatrix, 10000);

      MatrixMeta w3Meta = worker.getPSAgent().getMatrixMetaManager().getMatrixMeta("w3");
      MatrixMeta w4Meta = worker.getPSAgent().getMatrixMetaManager().getMatrixMeta("w4");
      assertEquals(w3Meta.getRowNum(), 1);
      assertEquals(w3Meta.getColNum(), 100000);
      assertEquals(w3Meta.getRowType(), RowType.T_DOUBLE_DENSE);
      assertEquals(w4Meta.getRowNum(), 1);
      assertEquals(w4Meta.getColNum(), 100000);
      assertEquals(w4Meta.getRowType(), RowType.T_DOUBLE_DENSE);
      w3Id = w3Meta.getId();
      w4Id = w4Meta.getId();

      AngelApplicationMaster angelAppMaster = LocalClusterContext.get().getMaster().getAppMaster();
      assertTrue(angelAppMaster != null);
      com.tencent.angel.master.MatrixMetaManager matrixMetaManager =
        angelAppMaster.getAppContext().getMatrixMetaManager();
      MatrixProto matrixw3Proto = matrixMetaManager.getMatrix("w3");
      MatrixProto matrixw4Proto = matrixMetaManager.getMatrix("w4");
      assertNotNull(matrixw3Proto);
      assertNotNull(matrixw4Proto);

      assertEquals(matrixw3Proto.getRowNum(), 1);
      assertEquals(matrixw3Proto.getColNum(), 100000);
      assertEquals(matrixw3Proto.getMatrixPartLocationCount(), 2);
      List<MatrixPartitionLocation> w3Parts = matrixw3Proto.getMatrixPartLocationList();
      assertEquals(w3Parts.get(0).getPsId(), ProtobufUtil.convertToIdProto(psId));
      assertEquals(w3Parts.get(0).getPart().getPartitionId(), 0);
      assertEquals(w3Parts.get(0).getPart().getStartRow(), 0);
      assertEquals(w3Parts.get(0).getPart().getEndRow(), 1);
      assertEquals(w3Parts.get(0).getPart().getStartCol(), 0);
      assertEquals(w3Parts.get(0).getPart().getEndCol(), 50000);
      assertEquals(w3Parts.get(1).getPart().getPartitionId(), 1);
      assertEquals(w3Parts.get(1).getPart().getStartRow(), 0);
      assertEquals(w3Parts.get(1).getPart().getEndRow(), 1);
      assertEquals(w3Parts.get(1).getPart().getStartCol(), 50000);
      assertEquals(w3Parts.get(1).getPart().getEndCol(), 100000);

      List<MatrixPartitionLocation> w4Parts = matrixw4Proto.getMatrixPartLocationList();
      assertEquals(w4Parts.get(0).getPsId(), ProtobufUtil.convertToIdProto(psId));
      assertEquals(w4Parts.get(0).getPart().getPartitionId(), 0);
      assertEquals(w4Parts.get(0).getPart().getStartRow(), 0);
      assertEquals(w4Parts.get(0).getPart().getEndRow(), 1);
      assertEquals(w4Parts.get(0).getPart().getStartCol(), 0);
      assertEquals(w4Parts.get(0).getPart().getEndCol(), 50000);
      assertEquals(w4Parts.get(1).getPart().getPartitionId(), 1);
      assertEquals(w4Parts.get(1).getPart().getStartRow(), 0);
      assertEquals(w4Parts.get(1).getPart().getEndRow(), 1);
      assertEquals(w4Parts.get(1).getPart().getStartCol(), 50000);
      assertEquals(w4Parts.get(1).getPart().getEndCol(), 100000);

      ParameterServer ps = LocalClusterContext.get().getPS(psAttempt0Id).getPS();
      MatrixPartitionManager matrixPartManager = ps.getMatrixPartitionManager();
      ServerPartition w3Part0 = matrixPartManager.getPartition(w3Id, 0);
      ServerPartition w3Part1 = matrixPartManager.getPartition(w3Id, 1);
      assertTrue(w3Part0 != null);
      assertTrue(w3Part1 != null);
      assertEquals(w3Part0.getPartitionKey().getStartRow(), 0);
      assertEquals(w3Part0.getPartitionKey().getEndRow(), 1);
      assertEquals(w3Part0.getPartitionKey().getStartCol(), 0);
      assertEquals(w3Part0.getPartitionKey().getEndCol(), 50000);
      assertEquals(w3Part1.getPartitionKey().getStartRow(), 0);
      assertEquals(w3Part1.getPartitionKey().getEndRow(), 1);
      assertEquals(w3Part1.getPartitionKey().getStartCol(), 50000);
      assertEquals(w3Part1.getPartitionKey().getEndCol(), 100000);

      ServerPartition w4Part0 = matrixPartManager.getPartition(w4Id, 0);
      ServerPartition w4Part1 = matrixPartManager.getPartition(w4Id, 1);
      assertTrue(w4Part0 != null);
      assertTrue(w4Part1 != null);
      assertEquals(w4Part0.getPartitionKey().getStartRow(), 0);
      assertEquals(w4Part0.getPartitionKey().getEndRow(), 1);
      assertEquals(w4Part0.getPartitionKey().getStartCol(), 0);
      assertEquals(w4Part0.getPartitionKey().getEndCol(), 50000);
      assertEquals(w4Part1.getPartitionKey().getStartRow(), 0);
      assertEquals(w4Part1.getPartitionKey().getEndRow(), 1);
      assertEquals(w4Part1.getPartitionKey().getStartCol(), 50000);
      assertEquals(w4Part1.getPartitionKey().getEndCol(), 100000);

      MatrixClient w4ClientForTask0 = worker.getPSAgent().getMatrixClient("w4", 0);
      MatrixClient w4ClientForTask1 = worker.getPSAgent().getMatrixClient("w4", 1);
      TaskContext task0Context = w4ClientForTask0.getTaskContext();
      TaskContext task1Context = w4ClientForTask1.getTaskContext();
      double[] delta = new double[100000];
      for (int i = 0; i < delta.length; i++) {
        delta[i] = 1.0;
      }

      int iterIndex = 0;
      while (iterIndex < 5) {
        DenseIntDoubleVector row1 = (DenseIntDoubleVector) w4ClientForTask0.getRow(0);
        double sum1 = sum(row1.getValues());
        LOG.info("taskid=" + task0Context.getIndex() + ", matrixId=" + w4ClientForTask0.getMatrixId()
          + ", rowIndex=0, local row sum=" + sum1);
        DenseIntDoubleVector deltaRow1 = new DenseIntDoubleVector(delta.length, delta);
        deltaRow1.setMatrixId(w4ClientForTask0.getMatrixId());
        deltaRow1.setRowId(0);
        w4ClientForTask0.increment(deltaRow1);
        w4ClientForTask0.clock().get();
        task0Context.increaseEpoch();

        DenseIntDoubleVector row2 = (DenseIntDoubleVector) w4ClientForTask1.getRow(0);
        double sum2 = sum(row2.getValues());
        LOG.info("taskid=" + task0Context.getIndex() + ", matrixId=" + w4ClientForTask1.getMatrixId()
          + ", rowIndex=0, local row sum=" + sum2);
        DenseIntDoubleVector deltaRow2 = new DenseIntDoubleVector(delta.length, delta);
        deltaRow2.setMatrixId(w4ClientForTask1.getMatrixId());
        deltaRow2.setRowId(0);
        w4ClientForTask1.increment(deltaRow2);
        w4ClientForTask1.clock().get();
        task1Context.increaseEpoch();
        iterIndex++;
      }

      AMTaskManager amTaskManager = angelAppMaster.getAppContext().getTaskManager();
      AMTask amTask0 = amTaskManager.getTask(task0Id);
      AMTask amTask1 = amTaskManager.getTask(task1Id);
      assertEquals(amTask0.getIteration(), 5);
      assertEquals(amTask1.getIteration(), 5);
      Int2IntOpenHashMap task0MatrixClocks = amTask0.getMatrixClocks();
      assertEquals(task0MatrixClocks.size(), 1);
      assertEquals(task0MatrixClocks.get(w4Id), 5);
      Int2IntOpenHashMap task1MatrixClocks = amTask1.getMatrixClocks();
      assertEquals(task1MatrixClocks.size(), 1);
      assertEquals(task1MatrixClocks.get(w4Id), 5);

      DenseIntDoubleVector row1 = (DenseIntDoubleVector) w4ClientForTask0.getRow(0);
      double sum1 = sum(row1.getValues());
      assertEquals(sum1, 1000000.0, 0.000001);
      DenseIntDoubleVector row2 = (DenseIntDoubleVector) w4ClientForTask1.getRow(0);
      double sum2 = sum(row2.getValues());
      assertEquals(sum2, 1000000.0, 0.000001);

      masterClient.releaseMatrix(w3Meta);
      Thread.sleep(10000);

      matrixw3Proto = matrixMetaManager.getMatrix("w3");
      assertTrue(matrixw3Proto == null);
      ServerMatrix sw3 = matrixPartManager.getMatrixIdMap().get(w3Id);
      assertTrue(sw3 == null);

      w4ClientForTask0.clock().get();
      w4ClientForTask1.clock().get();
      row1 = (DenseIntDoubleVector) w4ClientForTask0.getRow(0);
      sum1 = sum(row1.getValues());
      assertEquals(sum1, 1000000.0, 0.000001);
      row2 = (DenseIntDoubleVector) w4ClientForTask1.getRow(0);
      sum2 = sum(row2.getValues());
      assertEquals(sum2, 1000000.0, 0.000001);
    } catch (Exception x) {
      LOG.error("run testCreateMatrix failed ", x);
      throw x;
    }
  }

  private double sum(double[] args) {
    double sum = 0.0;
    for (int i = 0; i < args.length; i++) {
      sum += args[i];
    }
    return sum;
  }

  @AfterClass
  public static void stop() throws AngelException {
    try {
      LOG.info("stop local cluster");
      angelClient.stop();
    } catch (Exception x) {
      LOG.error("stop failed ", x);
      throw x;
    }
  }
}

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


package com.tencent.angel.worker;


import com.tencent.angel.client.AngelClient;
import com.tencent.angel.client.AngelClientFactory;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.conf.MatrixConf;
import com.tencent.angel.localcluster.LocalClusterContext;
import com.tencent.angel.master.DummyTask;
import com.tencent.angel.master.MasterServiceTest;
import com.tencent.angel.ml.math2.matrix.RBIntDoubleMatrix;
import com.tencent.angel.ml.math2.matrix.RBIntIntMatrix;
import com.tencent.angel.ml.math2.storage.*;
import com.tencent.angel.ml.math2.vector.IntDoubleVector;
import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ml.math2.vector.IntIntVector;
import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ps.PSAttemptId;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.psagent.matrix.MatrixClient;
import com.tencent.angel.psagent.matrix.transport.adapter.GetRowsResult;
import com.tencent.angel.psagent.matrix.transport.adapter.RowIndex;
import com.tencent.angel.worker.task.TaskId;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.mapred.lib.CombineTextInputFormat;
import org.apache.log4j.PropertyConfigurator;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;

@RunWith(MockitoJUnitRunner.class) @FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TransportTest {
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

  // Matrix parameters
  private static int ddRow = 10;
  private static int ddCol = 20;
  private static int diRow = 10;
  private static int diCol = 20;
  private static int dfRow = 10;
  private static int dfCol = 20;
  private static int sdRow = 10;
  private static int sdCol = 20;
  private static int siRow = 10;
  private static int siCol = 20;


  static {
    PropertyConfigurator.configure("../conf/log4j.properties");
  }

  @BeforeClass public static void setup() throws Exception {
    try {
      // Set basic configuration keys
      Configuration conf = new Configuration();
      conf.setBoolean("mapred.mapper.new-api", true);
      conf.setBoolean(AngelConf.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST, true);
      conf.set(AngelConf.ANGEL_TASK_USER_TASKCLASS, DummyTask.class.getName());

      // Use local deploy mode and dummy data spliter
      conf.set(AngelConf.ANGEL_DEPLOY_MODE, "LOCAL");
      conf.setBoolean(AngelConf.ANGEL_AM_USE_DUMMY_DATASPLITER, true);
      conf.set(AngelConf.ANGEL_INPUTFORMAT_CLASS, CombineTextInputFormat.class.getName());
      conf.set(AngelConf.ANGEL_SAVE_MODEL_PATH, LOCAL_FS + TMP_PATH + "/out");
      conf.set(AngelConf.ANGEL_TRAIN_DATA_PATH, LOCAL_FS + TMP_PATH + "/in");
      conf.set(AngelConf.ANGEL_LOG_PATH, LOCAL_FS + TMP_PATH + "/log");

      conf.setInt(AngelConf.ANGEL_WORKERGROUP_NUMBER, 1);
      conf.setInt(AngelConf.ANGEL_PS_NUMBER, 1);
      conf.setInt(AngelConf.ANGEL_WORKER_TASK_NUMBER, 1);

      // Create an Angel client
      angelClient = AngelClientFactory.get(conf);

      // Add different types of matrix
      MatrixContext matrix = new MatrixContext();
      matrix.setName("dense_double_mat");
      matrix.setRowNum(ddRow);
      matrix.setColNum(ddCol);
      matrix.setMaxRowNumInBlock(ddRow / 2);
      matrix.setMaxColNumInBlock(ddCol / 2);
      matrix.setRowType(RowType.T_DOUBLE_DENSE);
      matrix.set(MatrixConf.MATRIX_OPLOG_ENABLEFILTER, "false");
      matrix.set(MatrixConf.MATRIX_HOGWILD, "false");
      matrix.set(MatrixConf.MATRIX_AVERAGE, "false");
      angelClient.addMatrix(matrix);


      matrix = new MatrixContext();
      matrix.setName("dense_double_mat_1");
      matrix.setRowNum(ddRow);
      matrix.setColNum(ddCol);
      matrix.setMaxRowNumInBlock(ddRow / 2);
      matrix.setMaxColNumInBlock(ddCol / 2);
      matrix.setRowType(RowType.T_DOUBLE_DENSE);
      matrix.set(MatrixConf.MATRIX_OPLOG_ENABLEFILTER, "false");
      matrix.set(MatrixConf.MATRIX_HOGWILD, "false");
      matrix.set(MatrixConf.MATRIX_AVERAGE, "false");
      angelClient.addMatrix(matrix);

      matrix = new MatrixContext();
      matrix.setName("dense_int_mat");
      matrix.setRowNum(diRow);
      matrix.setColNum(diCol);
      matrix.setMaxRowNumInBlock(diRow / 2);
      matrix.setMaxColNumInBlock(diCol / 2);
      matrix.setRowType(RowType.T_INT_DENSE);
      matrix.set(MatrixConf.MATRIX_OPLOG_ENABLEFILTER, "false");
      matrix.set(MatrixConf.MATRIX_HOGWILD, "false");
      matrix.set(MatrixConf.MATRIX_AVERAGE, "false");
      angelClient.addMatrix(matrix);

      matrix = new MatrixContext();
      matrix.setName("dense_int_mat_1");
      matrix.setRowNum(diRow);
      matrix.setColNum(diCol);
      matrix.setMaxRowNumInBlock(diRow / 2);
      matrix.setMaxColNumInBlock(diCol / 2);
      matrix.setRowType(RowType.T_INT_DENSE);
      matrix.set(MatrixConf.MATRIX_OPLOG_ENABLEFILTER, "false");
      matrix.set(MatrixConf.MATRIX_HOGWILD, "false");
      matrix.set(MatrixConf.MATRIX_AVERAGE, "false");
      angelClient.addMatrix(matrix);

      matrix = new MatrixContext();
      matrix.setName("dense_float_mat");
      matrix.setRowNum(dfRow);
      matrix.setColNum(dfCol);
      matrix.setMaxRowNumInBlock(dfRow / 2);
      matrix.setMaxColNumInBlock(dfCol / 2);
      matrix.setRowType(RowType.T_FLOAT_DENSE);
      matrix.set(MatrixConf.MATRIX_OPLOG_ENABLEFILTER, "false");
      matrix.set(MatrixConf.MATRIX_HOGWILD, "false");
      matrix.set(MatrixConf.MATRIX_AVERAGE, "false");
      angelClient.addMatrix(matrix);

      angelClient.startPSServer();
      angelClient.run();
      Thread.sleep(10000);
      group0Id = new WorkerGroupId(0);
      worker0Id = new WorkerId(group0Id, 0);
      worker0Attempt0Id = new WorkerAttemptId(worker0Id, 0);
      task0Id = new TaskId(0);
      psId = new ParameterServerId(0);
      psAttempt0Id = new PSAttemptId(psId, 0);
    } catch (Exception x) {
      LOG.error("setup failed ", x);
      throw x;
    }
  }

  @Test public void testGetDenseDoubleMatrix() throws Exception {
    try {
      Worker worker = LocalClusterContext.get().getWorker(worker0Attempt0Id).getWorker();
      MatrixClient mat = worker.getPSAgent().getMatrixClient("dense_double_mat", 0);

      for (int rowId = 0; rowId < ddRow; rowId += 5) {
        IntDoubleVector row = (IntDoubleVector) mat.getRow(rowId);
        IntDoubleVector expect = new IntDoubleVector(ddCol, new IntDoubleDenseVectorStorage(ddCol));
        assertArrayEquals(row.getStorage().getValues(), expect.getStorage().getValues(), 0.0);

        IntDoubleVector update = new IntDoubleVector(ddCol, new IntDoubleDenseVectorStorage(ddCol));
        update.setMatrixId(mat.getMatrixId());
        update.setRowId(rowId);
        Random rand = new Random(System.currentTimeMillis());
        for (int i = 0; i < ddCol; i += 2)
          update.set(i, rand.nextDouble());
        mat.increment(update);
        mat.clock().get();

        row = (IntDoubleVector) mat.getRow(rowId);
        expect.iadd(update);
        assertArrayEquals(expect.getStorage().getValues(), row.getStorage().getValues(), 0.0);

        update = new IntDoubleVector(ddCol, new IntDoubleDenseVectorStorage(ddCol));
        update.setMatrixId(mat.getMatrixId());
        update.setRowId(rowId);
        for (int i = 0; i < ddCol; i += 3)
          update.set(i, rand.nextDouble());
        mat.increment(update);
        mat.clock().get();
        row = (IntDoubleVector) mat.getRow(rowId);
        expect.iadd(update);
        assertArrayEquals(expect.getStorage().getValues(), row.getStorage().getValues(), 0.0);
      }
    } catch (Exception x) {
      LOG.error("run testGetDenseDoubleMatrix failed ", x);
      throw x;
    }
  }

  @Test public void testGetFlowDenseDoubleMatrix() throws Exception {
    try {
      Worker worker = LocalClusterContext.get().getWorker(worker0Attempt0Id).getWorker();
      MatrixClient mat = worker.getPSAgent().getMatrixClient("dense_double_mat_1", 0);

      IntDoubleVector[] rows = new IntDoubleVector[ddRow];
      for (int i = 0; i < ddRow; i++) {
        rows[i] = new IntDoubleVector(ddCol, new IntDoubleDenseVectorStorage(ddCol));
      }
      RBIntDoubleMatrix expect = new RBIntDoubleMatrix(ddRow, ddCol, rows);

      RowIndex rowIndex = new RowIndex();
      for (int i = 0; i < ddRow; i++)
        rowIndex.addRowId(i);

      GetRowsResult result = mat.getRowsFlow(rowIndex, ddRow / 2);

      Vector row;
      while ((row = result.take()) != null) {
        LOG.info("===========get row index=" + row.getRowId());
        assertArrayEquals((expect.getRow(row.getRowId())).getStorage().getValues(),
          ((IntDoubleVector) row).getStorage().getValues(), 0.0);
      }

      Random rand = new Random(System.currentTimeMillis());
      for (int rowId = 0; rowId < ddRow; rowId++) {
        IntDoubleVector update = new IntDoubleVector(ddCol, new IntDoubleDenseVectorStorage(ddCol));

        for (int j = 0; j < ddCol; j += 3)
          update.set(j, rand.nextDouble());

        mat.increment(rowId, update);
        expect.getRow(rowId).iadd(update);
      }

      mat.clock().get();

      rowIndex = new RowIndex();
      for (int i = 0; i < ddRow; i++)
        rowIndex.addRowId(i);
      result = mat.getRowsFlow(rowIndex, 2);

      while ((row = result.take()) != null) {
        assertArrayEquals((expect.getRow(row.getRowId())).getStorage().getValues(),
          ((IntDoubleVector) row).getStorage().getValues(), 0.0);
      }

      rowIndex = new RowIndex();
      for (int i = 0; i < ddRow; i++)
        rowIndex.addRowId(i);
      result = mat.getRowsFlow(rowIndex, 2);

      while (true) {
        row = result.poll();
        if (result.isFetchOver() && row == null)
          break;

        if (row == null)
          continue;

        assertArrayEquals((expect.getRow(row.getRowId())).getStorage().getValues(),
          ((IntDoubleVector) row).getStorage().getValues(), 0.0);
      }
    } catch (Exception x) {
      LOG.error("run testGetFlowDenseDoubleMatrix failed ", x);
      throw x;
    }
  }

  @Test public void testGetDenseFloatMatrix() throws Exception {
    try {
      Worker worker = LocalClusterContext.get().getWorker(worker0Attempt0Id).getWorker();
      MatrixClient mat = worker.getPSAgent().getMatrixClient("dense_float_mat", 0);

      Random rand = new Random(System.currentTimeMillis());
      for (int rowId = 0; rowId < dfRow; rowId += (rand.nextInt(4) + 1)) {
        LOG.info("=================get row " + rowId);
        IntFloatVector getRow = (IntFloatVector) mat.getRow(rowId);
        IntFloatVector expect = new IntFloatVector(dfCol, new IntFloatDenseVectorStorage(dfCol));
        assertArrayEquals(getRow.getStorage().getValues(), expect.getStorage().getValues(), 0.0F);


        IntFloatVector update = new IntFloatVector(dfCol, new IntFloatDenseVectorStorage(dfCol));
        update.setRowId(rowId);

        for (int i = 0; i < ddCol; i += 2)
          update.set(i, rand.nextFloat());
        mat.increment(update);
        mat.clock().get();

        IntFloatVector row = (IntFloatVector) mat.getRow(rowId);
        expect.iadd(update);
        assertArrayEquals(expect.getStorage().getValues(), row.getStorage().getValues(), 0.0F);

        update = new IntFloatVector(dfCol, new IntFloatDenseVectorStorage(dfCol));
        update.setRowId(rowId);
        for (int i = 0; i < ddCol; i += 3)
          update.set(i, rand.nextFloat());
        mat.increment(update);
        mat.clock().get();
        row = (IntFloatVector) mat.getRow(rowId);

        expect.iadd(update);
        assertArrayEquals(expect.getStorage().getValues(), row.getStorage().getValues(), 0.0F);
      }
    } catch (Exception x) {
      LOG.error("run testGetDenseFloatMatrix failed ", x);
      throw x;
    }
  }

  @Test public void testGetDenseIntMatrix() throws Exception {
    try {
      Worker worker = LocalClusterContext.get().getWorker(worker0Attempt0Id).getWorker();
      MatrixClient mat = worker.getPSAgent().getMatrixClient("dense_int_mat", 0);

      Random rand = new Random(System.currentTimeMillis());
      for (int rowId = 0; rowId < diRow; rowId += rand.nextInt(5) + 1) {
        IntIntVector row = (IntIntVector) mat.getRow(rowId);
        IntIntVector expect = new IntIntVector(diCol, new IntIntDenseVectorStorage(diCol));
        assertArrayEquals(row.getStorage().getValues(), expect.getStorage().getValues());

        IntIntVector update = new IntIntVector(diCol, new IntIntDenseVectorStorage(diCol));
        update.setRowId(rowId);

        for (int i = 0; i < ddCol; i += 2)
          update.set(i, rand.nextInt());
        mat.increment(update);
        mat.clock().get();

        row = (IntIntVector) mat.getRow(rowId);
        expect.iadd(update);
        assertArrayEquals(expect.getStorage().getValues(), row.getStorage().getValues());

        update = new IntIntVector(diCol, new IntIntDenseVectorStorage(diCol));
        update.setRowId(rowId);
        for (int i = 0; i < diCol; i += 3)
          update.set(i, rand.nextInt());
        mat.increment(update);
        mat.clock().get();
        row = (IntIntVector) mat.getRow(rowId);
        expect.iadd(update);
        assertArrayEquals(expect.getStorage().getValues(), row.getStorage().getValues());
      }
    } catch (Exception x) {
      LOG.error("run testGetDenseIntMatrix failed ", x);
      throw x;
    }
  }

  @Test public void testGetFlowDenseIntMatrix() throws Exception {
    try {
      Worker worker = LocalClusterContext.get().getWorker(worker0Attempt0Id).getWorker();
      MatrixClient mat = worker.getPSAgent().getMatrixClient("dense_int_mat_1", 0);

      IntIntVector[] rows = new IntIntVector[diRow];
      for (int i = 0; i < rows.length; i++) {
        rows[i] = new IntIntVector(diCol, new IntIntDenseVectorStorage(diCol));
      }
      RBIntIntMatrix expect = new RBIntIntMatrix(diRow, -1, rows);
      //DenseIntMatrix expect = new DenseIntMatrix(diRow, diCol);

      RowIndex rowIndex = new RowIndex();
      for (int i = 0; i < diRow; i++) {
        rowIndex.addRowId(i);
      }


      GetRowsResult result = mat.getRowsFlow(rowIndex, diRow / 2);

      Vector row;
      while ((row = result.take()) != null) {
        LOG.info("rowId=" + row.getRowId());
        ((IntIntVector) expect.getRow(row.getRowId())).getStorage().getValues();
        ((IntIntVector) row).getStorage().getValues();

        assertArrayEquals(((IntIntVector) expect.getRow(row.getRowId())).getStorage().getValues(),
          ((IntIntVector) row).getStorage().getValues());
      }

      Random rand = new Random(System.currentTimeMillis());
      for (int rowId = 0; rowId < diRow; rowId++) {
        IntIntVector update = new IntIntVector(diCol, new IntIntDenseVectorStorage(diCol));

        for (int j = 0; j < ddCol; j += 3)
          update.set(j, rand.nextInt());

        mat.increment(rowId, update);
        expect.getRow(rowId).iadd(update);
      }

      mat.clock().get();

      rowIndex = new RowIndex();
      for (int i = 0; i < ddRow; i++)
        rowIndex.addRowId(i);
      result = mat.getRowsFlow(rowIndex, 2);

      while ((row = result.take()) != null) {
        assertArrayEquals(((IntIntVector) expect.getRow(row.getRowId())).getStorage().getValues(),
          ((IntIntVector) row).getStorage().getValues());
      }


      rowIndex = new RowIndex();
      for (int i = 0; i < ddRow; i++)
        rowIndex.addRowId(i);
      result = mat.getRowsFlow(rowIndex, 2);

      while (true) {
        row = result.poll();
        if (result.isFetchOver() && row == null)
          break;

        if (row == null)
          continue;

        assertArrayEquals(((IntIntVector) expect.getRow(row.getRowId())).getStorage().getValues(),
          ((IntIntVector) row).getStorage().getValues());
      }
    } catch (Exception x) {
      LOG.error("run testGetFlowDenseIntMatrix failed ", x);
      throw x;
    }
  }

  @AfterClass public static void stop() throws IOException {
    try {
      LOG.info("stop local cluster");
      angelClient.stop();
    } catch (Exception x) {
      LOG.error("stop failed ", x);
      throw x;
    }
  }
}

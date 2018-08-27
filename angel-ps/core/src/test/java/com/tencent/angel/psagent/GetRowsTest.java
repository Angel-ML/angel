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


package com.tencent.angel.psagent;

import com.tencent.angel.client.AngelClient;
import com.tencent.angel.client.AngelClientFactory;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.localcluster.LocalClusterContext;
import com.tencent.angel.ml.math2.storage.IntIntDenseVectorStorage;
import com.tencent.angel.ml.math2.vector.IntIntVector;
import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ps.PSAttemptId;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.psagent.matrix.MatrixClient;
import com.tencent.angel.psagent.matrix.transport.adapter.GetRowsResult;
import com.tencent.angel.psagent.matrix.transport.adapter.RowIndex;
import com.tencent.angel.worker.Worker;
import com.tencent.angel.worker.WorkerAttemptId;
import com.tencent.angel.worker.WorkerGroupId;
import com.tencent.angel.worker.WorkerId;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.log4j.PropertyConfigurator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by payniexiao on 2018/7/4.
 */
public class GetRowsTest {
  public static String DENSE_DOUBLE_MAT = "dense_double_mat";
  public static String DENSE_DOUBLE_MAT_COMP = "dense_double_mat_comp";
  public static String SPARSE_DOUBLE_MAT = "sparse_double_mat";
  public static String SPARSE_DOUBLE_MAT_COMP = "sparse_double_mat_comp";

  public static String DENSE_FLOAT_MAT = "dense_float_mat";
  public static String DENSE_FLOAT_MAT_COMP = "dense_float_mat_comp";
  public static String SPARSE_FLOAT_MAT = "sparse_float_mat";
  public static String SPARSE_FLOAT_MAT_COMP = "sparse_float_mat_comp";

  public static String DENSE_INT_MAT = "dense_int_mat";
  public static String DENSE_INT_MAT_COMP = "dense_int_mat_comp";
  public static String SPARSE_INT_MAT = "sparse_int_mat";
  public static String SPARSE_INT_MAT_COMP = "sparse_int_mat_comp";

  public static String DENSE_LONG_MAT = "dense_long_mat";
  public static String DENSE_LONG_MAT_COMP = "dense_long_mat_comp";
  public static String SPARSE_LONG_MAT = "sparse_long_mat";
  public static String SPARSE_LONG_MAT_COMP = "sparse_long_mat_comp";

  public static String DENSE_DOUBLE_LONG_MAT_COMP = "dense_double_long_mat_comp";
  public static String SPARSE_DOUBLE_LONG_MAT = "sparse_double_long_mat";
  public static String SPARSE_DOUBLE_LONG_MAT_COMP = "sparse_double_long_mat_comp";

  public static String DENSE_FLOAT_LONG_MAT_COMP = "dense_float_long_mat_comp";
  public static String SPARSE_FLOAT_LONG_MAT = "sparse_float_long_mat";
  public static String SPARSE_FLOAT_LONG_MAT_COMP = "sparse_float_long_mat_comp";

  public static String DENSE_INT_LONG_MAT_COMP = "dense_int_long_mat_comp";
  public static String SPARSE_INT_LONG_MAT = "sparse_int_long_mat";
  public static String SPARSE_INT_LONG_MAT_COMP = "sparse_int_long_mat_comp";

  public static String DENSE_LONG_LONG_MAT_COMP = "dense_long_long_mat_comp";
  public static String SPARSE_LONG_LONG_MAT = "sparse_long_long_mat";
  public static String SPARSE_LONG_LONG_MAT_COMP = "sparse_long_long_mat_comp";

  private static final Log LOG = LogFactory.getLog(GetRowTest.class);
  private static final String LOCAL_FS = LocalFileSystem.DEFAULT_FS;
  private static final String TMP_PATH = System.getProperty("java.io.tmpdir", "/tmp");
  private AngelClient angelClient;
  private ParameterServerId psId;
  private PSAttemptId psAttempt0Id;
  private WorkerId workerId;
  private WorkerAttemptId workerAttempt0Id;

  int feaNum = 100000;
  int nnz = 1000;
  int rowNum = 10;

  static {
    PropertyConfigurator.configure("../conf/log4j.properties");
  }

  @Before public void setup() throws Exception {
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
    conf.setInt(AngelConf.ANGEL_WORKER_TASK_NUMBER, 1);
    conf.setInt(AngelConf.ANGEL_MODEL_PARTITIONER_PARTITION_SIZE, 1000);
    conf.setInt(AngelConf.ANGEL_PSAGENT_CACHE_SYNC_TIMEINTERVAL_MS, 100);

    // get a angel client
    angelClient = AngelClientFactory.get(conf);

    // add comp dense float matrix
    MatrixContext dciMat = new MatrixContext();
    dciMat.setName(DENSE_INT_MAT);
    dciMat.setRowNum(rowNum);
    dciMat.setColNum(feaNum);
    dciMat.setMaxColNumInBlock(feaNum / 3);
    dciMat.setRowType(RowType.T_INT_DENSE);
    angelClient.addMatrix(dciMat);

    // add comp dense float matrix
    MatrixContext sciMat = new MatrixContext();
    sciMat.setName(SPARSE_INT_MAT);
    sciMat.setRowNum(rowNum);
    sciMat.setColNum(feaNum);
    sciMat.setMaxColNumInBlock(feaNum / 3);
    sciMat.setRowType(RowType.T_INT_SPARSE);
    angelClient.addMatrix(sciMat);

    // Start PS
    angelClient.startPSServer();
    // Start to run application
    angelClient.run();

    Thread.sleep(5000);

    psId = new ParameterServerId(0);
    psAttempt0Id = new PSAttemptId(psId, 0);

    WorkerGroupId workerGroupId = new WorkerGroupId(0);
    workerId = new WorkerId(workerGroupId, 0);
    workerAttempt0Id = new WorkerAttemptId(workerId, 0);
  }

  @Test public void testGetRows() throws Exception {
    testDenseInt();
    testSparseInt();
  }

  public void testDenseInt() throws Exception {
    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient client1 = worker.getPSAgent().getMatrixClient(DENSE_INT_MAT, 0);
    int matrixW1Id = client1.getMatrixId();

    for (int i = 0; i < rowNum; i++) {
      IntIntVector deltaVec = new IntIntVector(feaNum, new IntIntDenseVectorStorage(feaNum));
      for (int j = 0; j < feaNum; j++)
        deltaVec.set(j, i * j);

      deltaVec.setRowId(i);
      client1.increment(deltaVec);
    }


    client1.clock().get();

    RowIndex rowIndex = new RowIndex();
    for (int i = 0; i < rowNum; i++) {
      rowIndex.addRowId(i);
    }
    rowIndex.setMatrixId(client1.getMatrixId());

    GetRowsResult result = client1.getRowsFlow(rowIndex, 2);
    List<IntIntVector> rows = new ArrayList<>(rowNum);
    while (true) {
      Vector v = result.take();
      if (v == null) {
        break;
      }
      rows.add((IntIntVector) v);
    }

    assertEquals(rowNum, rows.size());
    for (int i = 0; i < rowNum; i++) {
      IntIntVector row = rows.get(i);
      int rowId = row.getRowId();
      LOG.info("row id=" + rowId);
      for (int j = 0; j < feaNum; j++) {
        assertEquals(rowId * j, row.get(j));
      }
    }
  }

  public void testSparseInt() throws Exception {
    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient client1 = worker.getPSAgent().getMatrixClient(SPARSE_INT_MAT, 0);
    int matrixW1Id = client1.getMatrixId();

    for (int i = 0; i < rowNum; i++) {
      IntIntVector deltaVec = new IntIntVector(feaNum, new IntIntDenseVectorStorage(feaNum));
      for (int j = 0; j < feaNum; j++)
        deltaVec.set(j, i * j);

      deltaVec.setRowId(i);
      client1.increment(deltaVec);
    }


    client1.clock().get();

    RowIndex rowIndex = new RowIndex();
    for (int i = 0; i < rowNum; i++) {
      rowIndex.addRowId(i);
    }
    rowIndex.setMatrixId(client1.getMatrixId());

    GetRowsResult result = client1.getRowsFlow(rowIndex, 2);
    List<IntIntVector> rows = new ArrayList<>(rowNum);
    while (true) {
      Vector v = result.take();
      if (v == null) {
        break;
      }
      rows.add((IntIntVector) v);
    }

    assertEquals(rowNum, rows.size());
    for (int i = 0; i < rowNum; i++) {
      IntIntVector row = rows.get(i);
      int rowId = row.getRowId();
      LOG.info("row id=" + rowId);
      for (int j = 0; j < feaNum; j++) {
        assertEquals(rowId * j, row.get(j));
      }
    }
  }

  public static int[] genIndexs(int feaNum, int nnz) {

    int[] sortedIndex = new int[nnz];
    Random random = new Random(System.currentTimeMillis());
    sortedIndex[0] = random.nextInt(feaNum / nnz);
    for (int i = 1; i < nnz; i++) {
      int rand = random.nextInt((feaNum - sortedIndex[i - 1]) / (nnz - i));
      if (rand == 0)
        rand = 1;
      sortedIndex[i] = rand + sortedIndex[i - 1];
    }

    return sortedIndex;
  }

  public static long[] genLongIndexs(long feaNum, int nnz) {
    long[] sortedIndex = new long[nnz];
    Random random = new Random(System.currentTimeMillis());
    for (int i = 1; i < nnz; i++) {
      sortedIndex[i] = Math.abs(random.nextLong()) % feaNum;
    }
    return sortedIndex;
  }

  @After public void stop() throws AngelException {
    LOG.info("stop local cluster");
    angelClient.stop();
  }
}

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
import com.tencent.angel.ml.math2.storage.*;
import com.tencent.angel.ml.math2.vector.*;
import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ps.PSAttemptId;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.ps.storage.partitioner.HashPartitioner;
import com.tencent.angel.psagent.matrix.MatrixClient;
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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertEquals;

public class IncrementRowHashTest {
  public static String DENSE_DOUBLE_MAT = "dense_double_mat";
  public static String SPARSE_DOUBLE_MAT = "sparse_double_mat";

  public static String DENSE_FLOAT_MAT = "dense_float_mat";
  public static String SPARSE_FLOAT_MAT = "sparse_float_mat";

  public static String DENSE_INT_MAT = "dense_int_mat";
  public static String SPARSE_INT_MAT = "sparse_int_mat";

  public static String DENSE_LONG_MAT = "dense_long_mat";
  public static String SPARSE_LONG_MAT = "sparse_long_mat";

  public static String SPARSE_DOUBLE_LONG_MAT = "sparse_double_long_mat";
  public static String SPARSE_FLOAT_LONG_MAT = "sparse_float_long_mat";

  public static String SPARSE_INT_LONG_MAT = "sparse_int_long_mat";
  public static String SPARSE_LONG_LONG_MAT = "sparse_long_long_mat";


  private static final Log LOG = LogFactory.getLog(GetRowTest.class);
  private static final String LOCAL_FS = LocalFileSystem.DEFAULT_FS;
  private static final String TMP_PATH = System.getProperty("java.io.tmpdir", "/tmp");
  private AngelClient angelClient;
  private ParameterServerId psId;
  private PSAttemptId psAttempt0Id;
  private WorkerId workerId;
  private WorkerAttemptId workerAttempt0Id;

  int feaNum = 1000000;
  int nnz = 1000000;
  int partNum = 10;

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

    conf.setInt(AngelConf.ANGEL_PSAGENT_CACHE_SYNC_TIMEINTERVAL_MS, 10);
    conf.setInt(AngelConf.ANGEL_WORKER_HEARTBEAT_INTERVAL_MS, 1000);
    conf.setInt(AngelConf.ANGEL_PS_HEARTBEAT_INTERVAL_MS, 1000);
    conf.setBoolean("use.new.split", true);
    conf.setInt(AngelConf.ANGEL_WORKER_MAX_ATTEMPTS, 1);
    conf.setInt(AngelConf.ANGEL_PS_MAX_ATTEMPTS, 1);

    // get a angel client
    angelClient = AngelClientFactory.get(conf);

    // add sparse double matrix
    MatrixContext sMat = new MatrixContext();
    sMat.setName(SPARSE_DOUBLE_MAT);
    sMat.setRowNum(1);
    sMat.setPartitionNum(partNum);
    sMat.setPartitionerClass(HashPartitioner.class);
    sMat.setRowType(RowType.T_DOUBLE_SPARSE);
    angelClient.addMatrix(sMat);


    // add sparse float matrix
    MatrixContext sfMat = new MatrixContext();
    sfMat.setName(SPARSE_FLOAT_MAT);
    sfMat.setRowNum(1);
    sfMat.setPartitionNum(partNum);
    sfMat.setPartitionerClass(HashPartitioner.class);
    sfMat.setRowType(RowType.T_FLOAT_SPARSE);
    angelClient.addMatrix(sfMat);

    // add sparse float matrix
    MatrixContext siMat = new MatrixContext();
    siMat.setName(SPARSE_INT_MAT);
    siMat.setRowNum(1);
    siMat.setPartitionNum(partNum);
    siMat.setPartitionerClass(HashPartitioner.class);
    siMat.setRowType(RowType.T_INT_SPARSE);
    angelClient.addMatrix(siMat);

    // add sparse long matrix
    MatrixContext slMat = new MatrixContext();
    slMat.setName(SPARSE_LONG_MAT);
    slMat.setRowNum(1);
    slMat.setPartitionNum(partNum);
    slMat.setPartitionerClass(HashPartitioner.class);
    slMat.setRowType(RowType.T_LONG_SPARSE);
    angelClient.addMatrix(slMat);

    // add sparse long-key double matrix
    MatrixContext dLongKeysMatrix = new MatrixContext();
    dLongKeysMatrix.setName(SPARSE_DOUBLE_LONG_MAT);
    dLongKeysMatrix.setRowNum(1);
    dLongKeysMatrix.setPartitionNum(partNum);
    dLongKeysMatrix.setPartitionerClass(HashPartitioner.class);
    dLongKeysMatrix.setRowType(RowType.T_DOUBLE_SPARSE_LONGKEY);
    angelClient.addMatrix(dLongKeysMatrix);

    // add sparse long-key float matrix
    MatrixContext slfMatrix = new MatrixContext();
    slfMatrix.setName(SPARSE_FLOAT_LONG_MAT);
    slfMatrix.setRowNum(1);
    slfMatrix.setPartitionNum(partNum);
    slfMatrix.setPartitionerClass(HashPartitioner.class);
    slfMatrix.setRowType(RowType.T_FLOAT_SPARSE_LONGKEY);
    angelClient.addMatrix(slfMatrix);

    // add sparse long-key int matrix
    MatrixContext sliMatrix = new MatrixContext();
    sliMatrix.setName(SPARSE_INT_LONG_MAT);
    sliMatrix.setRowNum(1);
    sliMatrix.setPartitionNum(partNum);
    sliMatrix.setPartitionerClass(HashPartitioner.class);
    sliMatrix.setRowType(RowType.T_INT_SPARSE_LONGKEY);
    angelClient.addMatrix(sliMatrix);

    // add sparse long-key long matrix
    MatrixContext sllMatrix = new MatrixContext();
    sllMatrix.setName(SPARSE_LONG_LONG_MAT);
    sllMatrix.setRowNum(1);
    sllMatrix.setPartitionNum(partNum);
    sllMatrix.setPartitionerClass(HashPartitioner.class);
    sllMatrix.setRowType(RowType.T_LONG_SPARSE_LONGKEY);
    angelClient.addMatrix(sllMatrix);

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

  @Test public void test() throws Exception {
    testSparseDoubleUDF();

    testSparseFloatUDF();

    testSparseIntUDF();

    testSparseLongUDF();

    testSparseDoubleLongKeyUDF();

    testSparseFloatLongKeyUDF();

    testSparseIntLongKeyUDF();

    testSparseLongLongKeyUDF();
  }


  public void testSparseDoubleLongKeyUDF() throws Exception {

    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient client1 = worker.getPSAgent().getMatrixClient(SPARSE_DOUBLE_LONG_MAT, 0);

    long[] index = genLongIndexs(feaNum, nnz);

    LongDoubleVector deltaVec =
        new LongDoubleVector(feaNum, new LongDoubleSparseVectorStorage(feaNum, nnz));
    for (int i = 0; i < feaNum; i++)
      deltaVec.set(i, i);
    deltaVec.setRowId(0);

    client1.increment(deltaVec, true);

    LongDoubleVector row = (LongDoubleVector) client1.getRow(0);
    for (long id : index) {
      //System.out.println("id=" + id + ", value=" + row.get(id));
      Assert.assertTrue(row.get(id) == deltaVec.get(id));
    }
  }


  public void testSparseFloatLongKeyUDF() throws Exception {

    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient client1 = worker.getPSAgent().getMatrixClient(SPARSE_FLOAT_LONG_MAT, 0);

    long[] index = genLongIndexs(feaNum, nnz);

    LongFloatVector deltaVec =
        new LongFloatVector(feaNum, new LongFloatSparseVectorStorage(feaNum, nnz));
    for (int i = 0; i < nnz; i++)
      deltaVec.set(index[i], index[i]);
    deltaVec.setRowId(0);

    client1.increment(deltaVec, true);

    LongFloatVector row = (LongFloatVector) client1.getRow(0);
    for (long id : index) {
      //System.out.println("id=" + id + ", value=" + row.get(id));
      Assert.assertTrue(row.get(id) == deltaVec.get(id));
    }

    //Assert.assertTrue(index.length == row.size());
  }

  public void testSparseLongLongKeyUDF() throws Exception {

    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient client1 = worker.getPSAgent().getMatrixClient(SPARSE_LONG_LONG_MAT, 0);

    long[] index = genLongIndexs(feaNum, nnz);

    LongLongVector deltaVec =
        new LongLongVector(feaNum, new LongLongSparseVectorStorage(feaNum, nnz));
    for (int i = 0; i < nnz; i++)
      deltaVec.set(index[i], index[i]);
    deltaVec.setRowId(0);

    client1.increment(deltaVec, true);

    LongLongVector row = (LongLongVector) client1.getRow(0);
    for (long id : index) {
      //System.out.println("id=" + id + ", value=" + row.get(id));
      Assert.assertEquals(row.get(id), deltaVec.get(id));
    }

    //Assert.assertTrue(index.length == row.size());
  }

  public void testSparseIntLongKeyUDF() throws Exception {

    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient client1 = worker.getPSAgent().getMatrixClient(SPARSE_INT_LONG_MAT, 0);

    long[] index = genLongIndexs(feaNum, nnz);

    LongIntVector deltaVec = new LongIntVector(feaNum, new LongIntSparseVectorStorage(feaNum, nnz));
    for (int i = 0; i < nnz; i++)
      deltaVec.set(index[i], (int) index[i]);
    deltaVec.setRowId(0);

    client1.increment(deltaVec, true);

    LongIntVector row = (LongIntVector) client1.getRow(0);
    for (long id : index) {
      //System.out.println("id=" + id + ", value=" + row.get(id));
      Assert.assertTrue(row.get(id) == deltaVec.get(id));
    }

    //Assert.assertTrue(index.length == row.size());
  }


  public void testDenseDoubleUDF() throws Exception {
    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient client1 = worker.getPSAgent().getMatrixClient(DENSE_DOUBLE_MAT, 0);

    int[] index = genIndexs(feaNum, nnz);

    IntDoubleVector deltaVec = new IntDoubleVector(feaNum, new IntDoubleDenseVectorStorage(feaNum));
    for (int i = 0; i < feaNum; i++)
      deltaVec.set(i, i);
    deltaVec.setRowId(0);

    client1.increment(deltaVec, true);

    IntDoubleVector row = (IntDoubleVector) client1.getRow(0);
    for (int id : index) {
      Assert.assertTrue(row.get(id) == deltaVec.get(id));
    }
    Assert.assertEquals(feaNum, row.size());
  }

  public void testSparseDoubleUDF() throws Exception {

    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient client1 = worker.getPSAgent().getMatrixClient(SPARSE_DOUBLE_MAT, 0);

    int[] index = genIndexs(feaNum, nnz);

    IntDoubleVector deltaVec =
        new IntDoubleVector(feaNum, new IntDoubleSparseVectorStorage(feaNum, nnz));
    for (int i = 0; i < index.length; i++) {
      deltaVec.set(index[i], index[i]);
    }
    deltaVec.setRowId(0);

    client1.increment(deltaVec, true);

    IntDoubleVector row = (IntDoubleVector) client1.getRow(0);
    for (int id : index) {
      //System.out.println("id=" + id + ", value=" + row.get(id));
      Assert.assertTrue(row.get(id) == deltaVec.get(id));
    }

    Assert.assertTrue(index.length == row.size());
  }

  public void testDenseFloatUDF() throws Exception {
    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient client1 = worker.getPSAgent().getMatrixClient(DENSE_FLOAT_MAT, 0);

    int[] index = genIndexs(feaNum, nnz);

    IntFloatVector deltaVec = new IntFloatVector(feaNum, new IntFloatDenseVectorStorage(feaNum));
    for (int i = 0; i < feaNum; i++)
      deltaVec.set(i, i);
    deltaVec.setRowId(0);

    client1.increment(deltaVec, true);

    IntFloatVector row = (IntFloatVector) client1.getRow(0);
    for (int id : index) {
      Assert.assertTrue(row.get(id) == deltaVec.get(id));
    }
    Assert.assertTrue(feaNum == row.size());

  }

  public void testSparseFloatUDF() throws Exception {

    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient client1 = worker.getPSAgent().getMatrixClient(SPARSE_FLOAT_MAT, 0);

    int[] index = genIndexs(feaNum, nnz);

    IntFloatVector deltaVec =
        new IntFloatVector(feaNum, new IntFloatSparseVectorStorage(feaNum, nnz));
    for (int i = 0; i < index.length; i++) {
      deltaVec.set(index[i], index[i]);
    }
    //for (int i = 0; i < feaNum; i++) {
    //  deltaVec.set(i, i);
    //}
    deltaVec.setRowId(0);

    client1.increment(deltaVec, true);

    IntFloatVector row = (IntFloatVector) client1.getRow(0);
    for (int id : index) {
      //System.out.println("id=" + id + ", value=" + row.get(id));
      assertEquals(row.get(id), deltaVec.get(id), 0.000001);
    }

    Assert.assertTrue(index.length == row.size());
  }

  public void testDenseIntUDF() throws Exception {
    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient client1 = worker.getPSAgent().getMatrixClient(DENSE_INT_MAT, 0);

    int[] index = genIndexs(feaNum, nnz);

    IntIntVector deltaVec = new IntIntVector(feaNum, new IntIntDenseVectorStorage(feaNum));
    for (int i = 0; i < feaNum; i++)
      deltaVec.set(i, i);
    deltaVec.setRowId(0);

    client1.increment(deltaVec, true);

    IntIntVector row = (IntIntVector) client1.getRow(0);
    for (int id : index) {
      Assert.assertTrue(row.get(id) == deltaVec.get(id));
    }
    Assert.assertTrue(feaNum == row.size());

  }

  public void testSparseIntUDF() throws Exception {

    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient client1 = worker.getPSAgent().getMatrixClient(SPARSE_INT_MAT, 0);

    int[] index = genIndexs(feaNum, nnz);

    IntIntVector deltaVec = new IntIntVector(feaNum, new IntIntSparseVectorStorage(feaNum, nnz));
    for (int i = 0; i < index.length; i++) {
      deltaVec.set(index[i], index[i]);
    }

    deltaVec.setRowId(0);

    client1.increment(deltaVec, true);

    IntIntVector row = (IntIntVector) client1.getRow(0);
    for (int id : index) {
      //System.out.println("id=" + id + ", value=" + row.get(id));
      Assert.assertTrue(row.get(id) == deltaVec.get(id));
    }

    Assert.assertTrue(index.length == row.size());
  }


  public void testDenseLongUDF() throws Exception {
    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient client1 = worker.getPSAgent().getMatrixClient(DENSE_LONG_MAT, 0);

    int[] index = genIndexs(feaNum, nnz);

    IntLongVector deltaVec = new IntLongVector(feaNum, new IntLongDenseVectorStorage(feaNum));
    for (int i = 0; i < feaNum; i++)
      deltaVec.set(i, i);
    deltaVec.setRowId(0);

    client1.increment(deltaVec, true);

    IntLongVector row = (IntLongVector) client1.getRow(0);
    for (int id : index) {
      //System.out.println("id=" + id + ", value=" + row.get(id));
      Assert.assertEquals(row.get(id), deltaVec.get(id));
    }
    Assert.assertTrue(feaNum == row.size());

  }

  public void testSparseLongUDF() throws Exception {

    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient client1 = worker.getPSAgent().getMatrixClient(SPARSE_LONG_MAT, 0);

    int[] index = genIndexs(feaNum, nnz);

    IntLongVector deltaVec = new IntLongVector(feaNum, new IntLongSparseVectorStorage(feaNum, nnz));
    for (int i = 0; i < index.length; i++) {
      deltaVec.set(index[i], index[i]);
    }
    deltaVec.setRowId(0);

    client1.increment(deltaVec, true);

    IntLongVector row = (IntLongVector) client1.getRow(0);
    for (int id : index) {
      //System.out.println("id=" + id + ", value=" + row.get(id));
      Assert.assertTrue(row.get(id) == deltaVec.get(id));
    }

    Assert.assertTrue(index.length == row.size());
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

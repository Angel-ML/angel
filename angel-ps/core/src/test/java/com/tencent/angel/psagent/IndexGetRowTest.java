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
import com.tencent.angel.ml.math2.VFactory;
import com.tencent.angel.ml.math2.matrix.RowBasedMatrix;
import com.tencent.angel.ml.math2.storage.*;
import com.tencent.angel.ml.math2.vector.*;
import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ml.matrix.psf.get.indexed.IndexGet;
import com.tencent.angel.ml.matrix.psf.get.indexed.IndexGetParam;
import com.tencent.angel.ml.matrix.psf.get.indexed.LongIndexGet;
import com.tencent.angel.ml.matrix.psf.get.indexed.LongIndexGetParam;
import com.tencent.angel.ps.PSAttemptId;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.psagent.matrix.MatrixClient;
import com.tencent.angel.psagent.matrix.oplog.cache.MatrixOpLog;
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

public class IndexGetRowTest {
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

  private static final Log LOG = LogFactory.getLog(IndexGetRowTest.class);
  private static final String LOCAL_FS = LocalFileSystem.DEFAULT_FS;
  private static final String TMP_PATH = System.getProperty("java.io.tmpdir", "/tmp");
  private AngelClient angelClient;
  private ParameterServerId psId;
  private PSAttemptId psAttempt0Id;
  private WorkerId workerId;
  private WorkerAttemptId workerAttempt0Id;

  int feaNum = 90000;
  int nnz = 9000;
  int modelSize = 90000;

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
    conf.setInt(AngelConf.ANGEL_MODEL_PARTITIONER_PARTITION_SIZE, 100000);

    conf.setInt(AngelConf.ANGEL_PSAGENT_CACHE_SYNC_TIMEINTERVAL_MS, 10);
    conf.setInt(AngelConf.ANGEL_WORKER_HEARTBEAT_INTERVAL_MS, 1000);
    conf.setInt(AngelConf.ANGEL_PS_HEARTBEAT_INTERVAL_MS, 1000);
    conf.setInt(AngelConf.ANGEL_WORKER_MAX_ATTEMPTS, 1);
    conf.setInt(AngelConf.ANGEL_PS_MAX_ATTEMPTS, 1);

    // get a angel client
    angelClient = AngelClientFactory.get(conf);

    // add dense double matrix
    MatrixContext dMat = new MatrixContext();
    dMat.setName(DENSE_DOUBLE_MAT);
    dMat.setRowNum(1);
    dMat.setColNum(feaNum);
    dMat.setMaxColNumInBlock(feaNum / 3);
    dMat.setRowType(RowType.T_DOUBLE_DENSE);
    dMat.setValidIndexNum(modelSize);
    angelClient.addMatrix(dMat);

    // add sparse double matrix
    MatrixContext sMat = new MatrixContext();
    sMat.setName(SPARSE_DOUBLE_MAT);
    sMat.setRowNum(1);
    sMat.setColNum(feaNum);
    sMat.setMaxColNumInBlock(feaNum / 3);
    sMat.setRowType(RowType.T_DOUBLE_SPARSE);
    sMat.setValidIndexNum(modelSize);
    angelClient.addMatrix(sMat);

    // add comp dense double matrix
    MatrixContext dcMat = new MatrixContext();
    dcMat.setName(DENSE_DOUBLE_MAT_COMP);
    dcMat.setRowNum(1);
    dcMat.setColNum(feaNum);
    dcMat.setMaxColNumInBlock(feaNum / 3);
    dcMat.setRowType(RowType.T_DOUBLE_DENSE_COMPONENT);
    dcMat.setValidIndexNum(modelSize);
    angelClient.addMatrix(dcMat);

    // add component sparse double matrix
    MatrixContext sCompMat = new MatrixContext();
    sCompMat.setName(SPARSE_DOUBLE_MAT_COMP);
    sCompMat.setRowNum(1);
    sCompMat.setColNum(feaNum);
    sCompMat.setMaxColNumInBlock(feaNum / 3);
    sCompMat.setRowType(RowType.T_DOUBLE_SPARSE_COMPONENT);
    sCompMat.setValidIndexNum(modelSize);
    angelClient.addMatrix(sCompMat);

    // add dense float matrix
    MatrixContext dfMat = new MatrixContext();
    dfMat.setName(DENSE_FLOAT_MAT);
    dfMat.setRowNum(1);
    dfMat.setColNum(feaNum);
    dfMat.setMaxColNumInBlock(feaNum / 3);
    dfMat.setRowType(RowType.T_FLOAT_DENSE);
    dfMat.setValidIndexNum(modelSize);
    angelClient.addMatrix(dfMat);

    // add comp dense float matrix
    MatrixContext dcfMat = new MatrixContext();
    dcfMat.setName(DENSE_FLOAT_MAT_COMP);
    dcfMat.setRowNum(1);
    dcfMat.setColNum(feaNum);
    dcfMat.setMaxColNumInBlock(feaNum / 3);
    dcfMat.setRowType(RowType.T_FLOAT_DENSE_COMPONENT);
    dcfMat.setValidIndexNum(modelSize);
    angelClient.addMatrix(dcfMat);

    // add sparse float matrix
    MatrixContext sfMat = new MatrixContext();
    sfMat.setName(SPARSE_FLOAT_MAT);
    sfMat.setRowNum(1);
    sfMat.setColNum(feaNum);
    sfMat.setMaxColNumInBlock(feaNum / 3);
    sfMat.setRowType(RowType.T_FLOAT_SPARSE);
    sfMat.setValidIndexNum(modelSize);
    angelClient.addMatrix(sfMat);

    // add component sparse float matrix
    MatrixContext sfCompMat = new MatrixContext();
    sfCompMat.setName(SPARSE_FLOAT_MAT_COMP);
    sfCompMat.setRowNum(1);
    sfCompMat.setColNum(feaNum);
    sfCompMat.setMaxColNumInBlock(feaNum / 3);
    sfCompMat.setRowType(RowType.T_FLOAT_SPARSE_COMPONENT);
    sfCompMat.setValidIndexNum(modelSize);
    angelClient.addMatrix(sfCompMat);

    // add dense float matrix
    MatrixContext diMat = new MatrixContext();
    diMat.setName(DENSE_INT_MAT);
    diMat.setRowNum(1);
    diMat.setColNum(feaNum);
    diMat.setMaxColNumInBlock(feaNum / 3);
    diMat.setRowType(RowType.T_INT_DENSE);
    diMat.setValidIndexNum(modelSize);
    angelClient.addMatrix(diMat);

    // add comp dense float matrix
    MatrixContext dciMat = new MatrixContext();
    dciMat.setName(DENSE_INT_MAT_COMP);
    dciMat.setRowNum(1);
    dciMat.setColNum(feaNum);
    dciMat.setMaxColNumInBlock(feaNum / 3);
    dciMat.setRowType(RowType.T_INT_DENSE_COMPONENT);
    dciMat.setValidIndexNum(modelSize);
    angelClient.addMatrix(dciMat);

    // add sparse float matrix
    MatrixContext siMat = new MatrixContext();
    siMat.setName(SPARSE_INT_MAT);
    siMat.setRowNum(1);
    siMat.setColNum(feaNum);
    siMat.setMaxColNumInBlock(feaNum / 3);
    siMat.setRowType(RowType.T_INT_SPARSE);
    siMat.setValidIndexNum(modelSize);
    angelClient.addMatrix(siMat);

    // add component sparse float matrix
    MatrixContext siCompMat = new MatrixContext();
    siCompMat.setName(SPARSE_INT_MAT_COMP);
    siCompMat.setRowNum(1);
    siCompMat.setColNum(feaNum);
    siCompMat.setMaxColNumInBlock(feaNum / 3);
    siCompMat.setRowType(RowType.T_INT_SPARSE_COMPONENT);
    siCompMat.setValidIndexNum(modelSize);
    angelClient.addMatrix(siCompMat);

    // add dense long matrix
    MatrixContext dlMat = new MatrixContext();
    dlMat.setName(DENSE_LONG_MAT);
    dlMat.setRowNum(1);
    dlMat.setColNum(feaNum);
    dlMat.setMaxColNumInBlock(feaNum / 3);
    dlMat.setRowType(RowType.T_LONG_DENSE);
    dlMat.setValidIndexNum(modelSize);
    angelClient.addMatrix(dlMat);

    // add comp dense long matrix
    MatrixContext dclMat = new MatrixContext();
    dclMat.setName(DENSE_LONG_MAT_COMP);
    dclMat.setRowNum(1);
    dclMat.setColNum(feaNum);
    dclMat.setMaxColNumInBlock(feaNum / 3);
    dclMat.setRowType(RowType.T_LONG_DENSE_COMPONENT);
    dclMat.setValidIndexNum(modelSize);
    angelClient.addMatrix(dclMat);

    // add sparse long matrix
    MatrixContext slMat = new MatrixContext();
    slMat.setName(SPARSE_LONG_MAT);
    slMat.setRowNum(1);
    slMat.setColNum(feaNum);
    slMat.setMaxColNumInBlock(feaNum / 3);
    slMat.setRowType(RowType.T_LONG_SPARSE);
    slMat.setValidIndexNum(modelSize);
    angelClient.addMatrix(slMat);

    // add component sparse long matrix
    MatrixContext slcMat = new MatrixContext();
    slcMat.setName(SPARSE_LONG_MAT_COMP);
    slcMat.setRowNum(1);
    slcMat.setColNum(feaNum);
    slcMat.setMaxColNumInBlock(feaNum / 3);
    slcMat.setRowType(RowType.T_LONG_SPARSE_COMPONENT);
    slcMat.setValidIndexNum(modelSize);
    angelClient.addMatrix(slcMat);

    // add comp dense long double matrix
    MatrixContext dldcMatrix = new MatrixContext();
    dldcMatrix.setName(DENSE_DOUBLE_LONG_MAT_COMP);
    dldcMatrix.setRowNum(1);
    dldcMatrix.setColNum(feaNum);
    dldcMatrix.setMaxColNumInBlock(feaNum / 3);
    dldcMatrix.setRowType(RowType.T_DOUBLE_DENSE_LONGKEY_COMPONENT);
    dldcMatrix.setValidIndexNum(modelSize);
    angelClient.addMatrix(dldcMatrix);

    // add sparse long-key double matrix
    MatrixContext dLongKeysMatrix = new MatrixContext();
    dLongKeysMatrix.setName(SPARSE_DOUBLE_LONG_MAT);
    dLongKeysMatrix.setRowNum(1);
    dLongKeysMatrix.setColNum(feaNum);
    dLongKeysMatrix.setMaxColNumInBlock(feaNum / 3);
    dLongKeysMatrix.setRowType(RowType.T_DOUBLE_SPARSE_LONGKEY);
    dLongKeysMatrix.setValidIndexNum(modelSize);
    angelClient.addMatrix(dLongKeysMatrix);

    // add component long-key sparse double matrix
    MatrixContext dLongKeysCompMatrix = new MatrixContext();
    dLongKeysCompMatrix.setName(SPARSE_DOUBLE_LONG_MAT_COMP);
    dLongKeysCompMatrix.setRowNum(1);
    dLongKeysCompMatrix.setColNum(feaNum);
    dLongKeysCompMatrix.setMaxColNumInBlock(feaNum / 3);
    dLongKeysCompMatrix.setRowType(RowType.T_DOUBLE_SPARSE_LONGKEY_COMPONENT);
    dLongKeysCompMatrix.setValidIndexNum(modelSize);
    angelClient.addMatrix(dLongKeysCompMatrix);

    // add component long-key sparse float matrix
    MatrixContext dlfcMatrix = new MatrixContext();
    dlfcMatrix.setName(DENSE_FLOAT_LONG_MAT_COMP);
    dlfcMatrix.setRowNum(1);
    dlfcMatrix.setColNum(feaNum);
    dlfcMatrix.setMaxColNumInBlock(feaNum / 3);
    dlfcMatrix.setRowType(RowType.T_FLOAT_DENSE_LONGKEY_COMPONENT);
    dlfcMatrix.setValidIndexNum(modelSize);
    angelClient.addMatrix(dlfcMatrix);

    // add sparse long-key float matrix
    MatrixContext slfMatrix = new MatrixContext();
    slfMatrix.setName(SPARSE_FLOAT_LONG_MAT);
    slfMatrix.setRowNum(1);
    slfMatrix.setColNum(feaNum);
    slfMatrix.setMaxColNumInBlock(feaNum / 3);
    slfMatrix.setRowType(RowType.T_FLOAT_SPARSE_LONGKEY);
    slfMatrix.setValidIndexNum(modelSize);
    angelClient.addMatrix(slfMatrix);

    // add component long-key sparse float matrix
    MatrixContext slfcMatrix = new MatrixContext();
    slfcMatrix.setName(SPARSE_FLOAT_LONG_MAT_COMP);
    slfcMatrix.setRowNum(1);
    slfcMatrix.setColNum(feaNum);
    slfcMatrix.setMaxColNumInBlock(feaNum / 3);
    slfcMatrix.setRowType(RowType.T_FLOAT_SPARSE_LONGKEY_COMPONENT);
    slfcMatrix.setValidIndexNum(modelSize);
    angelClient.addMatrix(slfcMatrix);

    // add component long-key sparse int matrix
    MatrixContext dlicMatrix = new MatrixContext();
    dlicMatrix.setName(DENSE_INT_LONG_MAT_COMP);
    dlicMatrix.setRowNum(1);
    dlicMatrix.setColNum(feaNum);
    dlicMatrix.setMaxColNumInBlock(feaNum / 3);
    dlicMatrix.setRowType(RowType.T_INT_DENSE_LONGKEY_COMPONENT);
    dlicMatrix.setValidIndexNum(modelSize);
    angelClient.addMatrix(dlicMatrix);

    // add sparse long-key int matrix
    MatrixContext sliMatrix = new MatrixContext();
    sliMatrix.setName(SPARSE_INT_LONG_MAT);
    sliMatrix.setRowNum(1);
    sliMatrix.setColNum(feaNum);
    sliMatrix.setMaxColNumInBlock(feaNum / 3);
    sliMatrix.setRowType(RowType.T_INT_SPARSE_LONGKEY);
    sliMatrix.setValidIndexNum(modelSize);
    angelClient.addMatrix(sliMatrix);

    // add component long-key sparse int matrix
    MatrixContext slicMatrix = new MatrixContext();
    slicMatrix.setName(SPARSE_INT_LONG_MAT_COMP);
    slicMatrix.setRowNum(1);
    slicMatrix.setColNum(feaNum);
    slicMatrix.setMaxColNumInBlock(feaNum / 3);
    slicMatrix.setRowType(RowType.T_INT_SPARSE_LONGKEY_COMPONENT);
    slicMatrix.setValidIndexNum(modelSize);
    angelClient.addMatrix(slicMatrix);

    // add component long-key sparse long matrix
    MatrixContext dllcMatrix = new MatrixContext();
    dllcMatrix.setName(DENSE_LONG_LONG_MAT_COMP);
    dllcMatrix.setRowNum(1);
    dllcMatrix.setColNum(feaNum);
    dllcMatrix.setMaxColNumInBlock(feaNum / 3);
    dllcMatrix.setRowType(RowType.T_LONG_DENSE_LONGKEY_COMPONENT);
    dllcMatrix.setValidIndexNum(modelSize);
    angelClient.addMatrix(dllcMatrix);

    // add sparse long-key long matrix
    MatrixContext sllMatrix = new MatrixContext();
    sllMatrix.setName(SPARSE_LONG_LONG_MAT);
    sllMatrix.setRowNum(1);
    sllMatrix.setColNum(feaNum);
    sllMatrix.setMaxColNumInBlock(feaNum / 3);
    sllMatrix.setRowType(RowType.T_LONG_SPARSE_LONGKEY);
    sllMatrix.setValidIndexNum(modelSize);
    angelClient.addMatrix(sllMatrix);

    // add component long-key sparse long matrix
    MatrixContext sllcMatrix = new MatrixContext();
    sllcMatrix.setName(SPARSE_LONG_LONG_MAT_COMP);
    sllcMatrix.setRowNum(1);
    sllcMatrix.setColNum(feaNum);
    sllcMatrix.setMaxColNumInBlock(feaNum / 3);
    sllcMatrix.setRowType(RowType.T_LONG_SPARSE_LONGKEY_COMPONENT);
    sllcMatrix.setValidIndexNum(modelSize);
    angelClient.addMatrix(sllcMatrix);

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
    testDenseDoubleUDF();
    testSparseDoubleUDF();

    testDenseDoubleCompUDF();
    testSparseDoubleCompUDF();

    testDenseFloatUDF();
    testSparseFloatUDF();

    testDenseFloatCompUDF();
    testSparseFloatCompUDF();

    testDenseIntUDF();
    testSparseIntUDF();

    testDenseIntCompUDF();
    testSparseIntCompUDF();

    testDenseLongUDF();
    testSparseLongUDF();

    testDenseLongCompUDF();
    testSparseLongCompUDF();

    testSparseDoubleLongKeyUDF();
    testSparseDoubleLongKeyCompUDF();

    testSparseFloatLongKeyUDF();
    testSparseFloatLongKeyCompUDF();

    testSparseIntLongKeyUDF();
    testSparseIntLongKeyCompUDF();

    testSparseLongLongKeyUDF();
    testSparseLongLongKeyCompUDF();
  }


  public void testSparseDoubleLongKeyUDF() throws Exception {

    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient client1 = worker.getPSAgent().getMatrixClient(SPARSE_DOUBLE_LONG_MAT, 0);
    int matrixW1Id = client1.getMatrixId();

    long[] index = genLongIndexs(feaNum, nnz);

    LongDoubleVector deltaVec =
      new LongDoubleVector(feaNum, new LongDoubleSparseVectorStorage(feaNum, nnz));
    for (int i = 0; i < feaNum; i++)
      deltaVec.set(i, i);
    deltaVec.setRowId(0);

    client1.increment(deltaVec);
    client1.clock().get();

    LongIndexGet func = new LongIndexGet(new LongIndexGetParam(matrixW1Id, 0, index));

    LongDoubleVector row = (LongDoubleVector) client1.get(0, index);
    for (long id : index) {
      //System.out.println("id=" + id + ", value=" + row.get(id));
      Assert.assertTrue(row.get(id) == deltaVec.get(id));
    }

    //Assert.assertTrue(index.length == row.size());
  }


  public void testSparseDoubleLongKeyCompUDF() throws Exception {
    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient client1 = worker.getPSAgent().getMatrixClient(SPARSE_DOUBLE_LONG_MAT_COMP, 0);
    int matrixW1Id = client1.getMatrixId();
    long blockColNum =
      worker.getPSAgent().getMatrixMetaManager().getMatrixMeta(matrixW1Id).getBlockColNum();

    long[] index = genLongIndexs(feaNum, nnz);

    int num = (int) ((feaNum + blockColNum - 1) / blockColNum);
    LongDoubleVector[] vectors = new LongDoubleVector[num];
    for (int i = 0; i < num; i++) {
      vectors[i] = new LongDoubleVector(blockColNum,
        new LongDoubleSparseVectorStorage(blockColNum, nnz / num));
    }
    CompLongDoubleVector deltaVec = new CompLongDoubleVector(feaNum, vectors, blockColNum);
    for (long i = 0; i < feaNum; i++)
      deltaVec.set(i, i);
    deltaVec.setRowId(0);

    client1.increment(deltaVec);
    Thread.sleep(1000);
    MatrixOpLog opLog = worker.getPSAgent().getOpLogCache().getLog(matrixW1Id);
    RowBasedMatrix<CompLongDoubleVector> matrix =
      (RowBasedMatrix<CompLongDoubleVector>) opLog.getMatrix();
    CompLongDoubleVector mergedRow = matrix.getRow(0);
    for (long id : index) {
      //System.out.println("id=" + id + ", value=" + mergedRow.get(id));
      Assert.assertEquals(mergedRow.get(id), deltaVec.get(id), 0.0000000001);
    }

    client1.clock().get();

    LongIndexGet func = new LongIndexGet(new LongIndexGetParam(matrixW1Id, 0, index));

    CompLongDoubleVector row = (CompLongDoubleVector) client1.get(0, index);
    for (long id : index) {
      //System.out.println("id=" + id + ", value=" + row.get(id));
      Assert.assertEquals(row.get(id), deltaVec.get(id), 0.0000000001);
    }

    //Assert.assertTrue(index.length == row.size());
  }

  public void testSparseFloatLongKeyUDF() throws Exception {

    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient client1 = worker.getPSAgent().getMatrixClient(SPARSE_FLOAT_LONG_MAT, 0);
    int matrixW1Id = client1.getMatrixId();

    long[] index = genLongIndexs(feaNum, nnz);

    LongFloatVector deltaVec =
      new LongFloatVector(feaNum, new LongFloatSparseVectorStorage(feaNum, nnz));
    for (int i = 0; i < nnz; i++)
      deltaVec.set(index[i], index[i]);
    deltaVec.setRowId(0);

    client1.increment(deltaVec);
    client1.clock().get();

    LongIndexGet func = new LongIndexGet(new LongIndexGetParam(matrixW1Id, 0, index));

    LongFloatVector row = (LongFloatVector) client1.get(0, index);
    for (long id : index) {
      //System.out.println("id=" + id + ", value=" + row.get(id));
      Assert.assertTrue(row.get(id) == deltaVec.get(id));
    }

    //Assert.assertTrue(index.length == row.size());
  }

  public void testSparseFloatLongKeyCompUDF() throws Exception {
    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient client1 = worker.getPSAgent().getMatrixClient(SPARSE_FLOAT_LONG_MAT_COMP, 0);
    int matrixW1Id = client1.getMatrixId();
    long blockColNum =
      worker.getPSAgent().getMatrixMetaManager().getMatrixMeta(matrixW1Id).getBlockColNum();

    long[] index = genLongIndexs(feaNum, nnz);

    int num = (int) ((feaNum + blockColNum - 1) / blockColNum);
    LongFloatVector[] vectors = new LongFloatVector[num];
    for (int i = 0; i < num; i++) {
      vectors[i] =
        new LongFloatVector(blockColNum, new LongFloatSparseVectorStorage(blockColNum, nnz / num));
    }
    CompLongFloatVector deltaVec = new CompLongFloatVector(feaNum, vectors, blockColNum);
    for (int i = 0; i < nnz; i++)
      deltaVec.set(index[i], index[i]);
    deltaVec.setRowId(0);

    client1.increment(deltaVec);
    Thread.sleep(1000);
    MatrixOpLog opLog = worker.getPSAgent().getOpLogCache().getLog(matrixW1Id);
    RowBasedMatrix<CompLongFloatVector> matrix =
      (RowBasedMatrix<CompLongFloatVector>) opLog.getMatrix();
    CompLongFloatVector mergedRow = matrix.getRow(0);
    for (long id : index) {
      //System.out.println("id=" + id + ", value=" + mergedRow.get(id));
      Assert.assertEquals(mergedRow.get(id), deltaVec.get(id), 0.0000000001);
    }

    client1.clock().get();

    LongIndexGet func = new LongIndexGet(new LongIndexGetParam(matrixW1Id, 0, index));

    CompLongFloatVector row = (CompLongFloatVector) client1.get(0, index);
    for (long id : index) {
      //System.out.println("id=" + id + ", value=" + row.get(id));
      Assert.assertEquals(row.get(id), deltaVec.get(id), 0.0000000001);
    }

    //Assert.assertTrue(index.length == row.size());
  }

  public void testSparseLongLongKeyUDF() throws Exception {

    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient client1 = worker.getPSAgent().getMatrixClient(SPARSE_LONG_LONG_MAT, 0);
    int matrixW1Id = client1.getMatrixId();

    long[] index = genLongIndexs(feaNum, nnz);

    LongLongVector deltaVec =
      new LongLongVector(feaNum, new LongLongSparseVectorStorage(feaNum, nnz));
    for (int i = 0; i < nnz; i++)
      deltaVec.set(index[i], index[i]);
    deltaVec.setRowId(0);

    client1.increment(deltaVec);
    client1.clock().get();

    LongIndexGet func = new LongIndexGet(new LongIndexGetParam(matrixW1Id, 0, index));

    LongLongVector row = (LongLongVector) client1.get(0, index);
    for (long id : index) {
      //System.out.println("id=" + id + ", value=" + row.get(id));
      Assert.assertTrue(row.get(id) == deltaVec.get(id));
    }

    //Assert.assertTrue(index.length == row.size());
  }

  public void testSparseLongLongKeyCompUDF() throws Exception {
    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient client1 = worker.getPSAgent().getMatrixClient(SPARSE_LONG_LONG_MAT_COMP, 0);
    int matrixW1Id = client1.getMatrixId();
    long blockColNum =
      worker.getPSAgent().getMatrixMetaManager().getMatrixMeta(matrixW1Id).getBlockColNum();

    long[] index = genLongIndexs(feaNum, nnz);

    int num = (int) ((feaNum + blockColNum - 1) / blockColNum);
    LongLongVector[] vectors = new LongLongVector[num];
    for (int i = 0; i < num; i++) {
      vectors[i] =
        new LongLongVector(blockColNum, new LongLongSparseVectorStorage(blockColNum, nnz / num));
    }
    CompLongLongVector deltaVec = new CompLongLongVector(feaNum, vectors, blockColNum);
    for (int i = 0; i < nnz; i++)
      deltaVec.set(index[i], index[i]);
    deltaVec.setRowId(0);

    client1.increment(deltaVec);
    Thread.sleep(1000);
    MatrixOpLog opLog = worker.getPSAgent().getOpLogCache().getLog(matrixW1Id);
    RowBasedMatrix<CompLongLongVector> matrix =
      (RowBasedMatrix<CompLongLongVector>) opLog.getMatrix();
    CompLongLongVector mergedRow = matrix.getRow(0);
    for (long id : index) {
      //System.out.println("id=" + id + ", value=" + mergedRow.get(id));
      Assert.assertEquals(mergedRow.get(id), deltaVec.get(id), 0.0000000001);
    }

    client1.clock().get();

    LongIndexGet func = new LongIndexGet(new LongIndexGetParam(matrixW1Id, 0, index));

    CompLongLongVector row = (CompLongLongVector) client1.get(0, index);
    for (long id : index) {
      //System.out.println("id=" + id + ", value=" + row.get(id));
      Assert.assertEquals(row.get(id), deltaVec.get(id), 0.0000000001);
    }

    //Assert.assertTrue(index.length == row.size());
  }

  public void testSparseIntLongKeyUDF() throws Exception {

    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient client1 = worker.getPSAgent().getMatrixClient(SPARSE_INT_LONG_MAT, 0);
    int matrixW1Id = client1.getMatrixId();

    long[] index = genLongIndexs(feaNum, nnz);

    LongIntVector deltaVec = new LongIntVector(feaNum, new LongIntSparseVectorStorage(feaNum, nnz));
    for (int i = 0; i < nnz; i++)
      deltaVec.set(index[i], (int) index[i]);
    deltaVec.setRowId(0);

    client1.increment(deltaVec);
    client1.clock().get();

    LongIndexGet func = new LongIndexGet(new LongIndexGetParam(matrixW1Id, 0, index));

    LongIntVector row = (LongIntVector) client1.get(0, index);
    for (long id : index) {
      //System.out.println("id=" + id + ", value=" + row.get(id));
      Assert.assertTrue(row.get(id) == deltaVec.get(id));
    }

    //Assert.assertTrue(index.length == row.size());
  }

  public void testSparseIntLongKeyCompUDF() throws Exception {
    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient client1 = worker.getPSAgent().getMatrixClient(SPARSE_INT_LONG_MAT_COMP, 0);
    int matrixW1Id = client1.getMatrixId();
    long blockColNum =
      worker.getPSAgent().getMatrixMetaManager().getMatrixMeta(matrixW1Id).getBlockColNum();

    long[] index = genLongIndexs(feaNum, nnz);

    int num = (int) ((feaNum + blockColNum - 1) / blockColNum);
    LongIntVector[] vectors = new LongIntVector[num];
    for (int i = 0; i < num; i++) {
      vectors[i] =
        new LongIntVector(blockColNum, new LongIntSparseVectorStorage(blockColNum, nnz / num));
    }
    CompLongIntVector deltaVec = new CompLongIntVector(feaNum, vectors, blockColNum);
    for (int i = 0; i < nnz; i++)
      deltaVec.set(index[i], (int) index[i]);
    deltaVec.setRowId(0);

    client1.increment(deltaVec);
    Thread.sleep(1000);
    MatrixOpLog opLog = worker.getPSAgent().getOpLogCache().getLog(matrixW1Id);
    RowBasedMatrix<CompLongIntVector> matrix =
      (RowBasedMatrix<CompLongIntVector>) opLog.getMatrix();
    CompLongIntVector mergedRow = matrix.getRow(0);
    for (long id : index) {
      //System.out.println("id=" + id + ", value=" + mergedRow.get(id));
      Assert.assertEquals(mergedRow.get(id), deltaVec.get(id), 0.0000000001);
    }

    client1.clock().get();

    LongIndexGet func = new LongIndexGet(new LongIndexGetParam(matrixW1Id, 0, index));

    CompLongIntVector row = (CompLongIntVector) client1.get(0, index);
    for (long id : index) {
      //System.out.println("id=" + id + ", value=" + row.get(id));
      Assert.assertEquals(row.get(id), deltaVec.get(id), 0.0000000001);
    }

    //Assert.assertTrue(index.length == row.size());
  }


  public void testDenseDoubleUDF() throws Exception {
    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient client1 = worker.getPSAgent().getMatrixClient(DENSE_DOUBLE_MAT, 0);
    int matrixW1Id = client1.getMatrixId();

    int[] index = genIndexs(feaNum, nnz);

    IntDoubleVector deltaVec = new IntDoubleVector(feaNum, new IntDoubleDenseVectorStorage(feaNum));
    for (int i = 0; i < feaNum; i++)
      deltaVec.set(i, i);
    deltaVec.setRowId(0);

    client1.increment(deltaVec);
    client1.clock().get();

    //IndexGet func = new IndexGet(new IndexGetParam(matrixW1Id, 0, index));
    //IntDoubleVector row = (IntDoubleVector) ((GetRowResult) client1.get(func)).getRow();

    IntDoubleVector row = (IntDoubleVector) client1.get(0, index);
    for (int id : index) {
      Assert.assertTrue(row.get(id) == deltaVec.get(id));
    }
    Assert.assertTrue(index.length == row.size());

  }

  public void testSparseDoubleUDF() throws Exception {

    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient client1 = worker.getPSAgent().getMatrixClient(SPARSE_DOUBLE_MAT, 0);
    int matrixW1Id = client1.getMatrixId();

    int[] index = genIndexs(feaNum, nnz);

    IntDoubleVector deltaVec =
      new IntDoubleVector(feaNum, new IntDoubleSparseVectorStorage(feaNum, nnz));
    for (int i = 0; i < index.length; i++) {
      deltaVec.set(index[i], index[i]);
    }
    //for (int i = 0; i < feaNum; i++) {
    //  deltaVec.set(i, i);
    //}

    deltaVec.setRowId(0);

    client1.increment(deltaVec);
    client1.clock().get();

    //IndexGet func = new IndexGet(new IndexGetParam(matrixW1Id, 0, index));

    //IntDoubleVector row = (IntDoubleVector) ((GetRowResult) client1.get(func)).getRow();
    IntDoubleVector row = (IntDoubleVector) client1.get(0, index);
    for (int id : index) {
      //System.out.println("id=" + id + ", value=" + row.get(id));
      Assert.assertTrue(row.get(id) == deltaVec.get(id));
    }

    Assert.assertTrue(index.length == row.size());
  }

  public void testDenseDoubleCompUDF() throws Exception {
    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient client1 = worker.getPSAgent().getMatrixClient(DENSE_DOUBLE_MAT_COMP, 0);
    int matrixW1Id = client1.getMatrixId();

    int[] index = genIndexs(feaNum, nnz);

    long blockColNum =
      worker.getPSAgent().getMatrixMetaManager().getMatrixMeta(matrixW1Id).getBlockColNum();
    int partNum = (feaNum + (int) blockColNum - 1) / (int) blockColNum;
    IntDoubleVector[] subVecs = new IntDoubleVector[partNum];
    for (int i = 0; i < partNum; i++) {
      subVecs[i] = VFactory.denseDoubleVector((int) blockColNum);
    }

    CompIntDoubleVector deltaVec = new CompIntDoubleVector(feaNum, subVecs, (int) blockColNum);
    for (int i = 0; i < feaNum; i++)
      deltaVec.set(i, i);
    deltaVec.setRowId(0);

    client1.increment(deltaVec, false);
    client1.clock().get();

    IndexGet func = new IndexGet(new IndexGetParam(matrixW1Id, 0, index));
    CompIntDoubleVector row = (CompIntDoubleVector) client1.get(0, index);
    for (int id : index) {
      //LOG.info("id=" + id + ", value=" + row.get(id));
      Assert.assertEquals(row.get(id), deltaVec.get(id), 0.0000000001);
    }
    Assert.assertTrue(index.length == row.size());

  }

  public void testSparseDoubleCompUDF() throws Exception {

    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient client1 = worker.getPSAgent().getMatrixClient(SPARSE_DOUBLE_MAT_COMP, 0);
    int matrixW1Id = client1.getMatrixId();

    int[] index = genIndexs(feaNum, nnz);

    long blockColNum =
      worker.getPSAgent().getMatrixMetaManager().getMatrixMeta(matrixW1Id).getBlockColNum();
    int partNum = (feaNum + (int) blockColNum - 1) / (int) blockColNum;
    IntDoubleVector[] subVecs = new IntDoubleVector[partNum];
    for (int i = 0; i < partNum; i++) {
      subVecs[i] = VFactory.sparseDoubleVector((int) blockColNum, nnz / partNum);
    }

    CompIntDoubleVector deltaVec = new CompIntDoubleVector(feaNum, subVecs, (int) blockColNum);

    //CompSparseDoubleVector deltaVec = new CompSparseDoubleVector(matrixW1Id, 0, feaNum, feaNum);
    for (int i = 0; i < nnz; i++)
      deltaVec.set(index[i], index[i]);
    deltaVec.setRowId(0);

    client1.increment(deltaVec);
    client1.clock().get();

    IndexGet func = new IndexGet(new IndexGetParam(matrixW1Id, 0, index));

    CompIntDoubleVector row = (CompIntDoubleVector) client1.get(0, index);
    for (int id : index) {
      //System.out.println("id=" + id + ", value=" + row.get(id));
      Assert.assertTrue(row.get(id) == deltaVec.get(id));
    }

    Assert.assertTrue(index.length == row.size());
  }

  public void testDenseFloatUDF() throws Exception {
    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient client1 = worker.getPSAgent().getMatrixClient(DENSE_FLOAT_MAT, 0);
    int matrixW1Id = client1.getMatrixId();

    int[] index = genIndexs(feaNum, nnz);

    IntFloatVector deltaVec = new IntFloatVector(feaNum, new IntFloatDenseVectorStorage(feaNum));
    for (int i = 0; i < feaNum; i++)
      deltaVec.set(i, i);
    deltaVec.setRowId(0);

    client1.increment(deltaVec);
    client1.clock().get();

    //IndexGet func = new IndexGet(new IndexGetParam(matrixW1Id, 0, index));
    IntFloatVector row = (IntFloatVector) client1.get(0, index);
    for (int id : index) {
      Assert.assertTrue(row.get(id) == deltaVec.get(id));
    }
    Assert.assertTrue(index.length == row.size());

  }

  public void testSparseFloatUDF() throws Exception {

    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient client1 = worker.getPSAgent().getMatrixClient(SPARSE_FLOAT_MAT, 0);
    int matrixW1Id = client1.getMatrixId();

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

    client1.increment(deltaVec);
    client1.clock().get();

    //IndexGet func = new IndexGet(new IndexGetParam(matrixW1Id, 0, index));

    //IntFloatVector row = (IntFloatVector) ((GetRowResult) client1.get(func)).getRow();
    IntFloatVector row = (IntFloatVector) client1.get(0, index);
    for (int id : index) {
      //System.out.println("id=" + id + ", value=" + row.get(id));
      assertEquals(row.get(id), deltaVec.get(id), 0.000001);
    }

    Assert.assertTrue(index.length == row.size());
  }

  public void testDenseFloatCompUDF() throws Exception {
    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient client1 = worker.getPSAgent().getMatrixClient(DENSE_FLOAT_MAT_COMP, 0);
    int matrixW1Id = client1.getMatrixId();

    int[] index = genIndexs(feaNum, nnz);

    long blockColNum =
      worker.getPSAgent().getMatrixMetaManager().getMatrixMeta(matrixW1Id).getBlockColNum();
    int partNum = (feaNum + (int) blockColNum - 1) / (int) blockColNum;
    IntFloatVector[] subVecs = new IntFloatVector[partNum];
    for (int i = 0; i < partNum; i++) {
      subVecs[i] = VFactory.denseFloatVector((int) blockColNum);
    }

    CompIntFloatVector deltaVec = new CompIntFloatVector(feaNum, subVecs, (int) blockColNum);
    for (int i = 0; i < feaNum; i++)
      deltaVec.set(i, i);
    deltaVec.setRowId(0);

    client1.increment(deltaVec);
    client1.clock().get();

    IndexGet func = new IndexGet(new IndexGetParam(matrixW1Id, 0, index));
    CompIntFloatVector row = (CompIntFloatVector) client1.get(0, index);
    for (int id : index) {
      Assert.assertTrue(row.get(id) == deltaVec.get(id));
    }
    Assert.assertTrue(index.length == row.size());

  }

  public void testSparseFloatCompUDF() throws Exception {

    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient client1 = worker.getPSAgent().getMatrixClient(SPARSE_FLOAT_MAT_COMP, 0);
    int matrixW1Id = client1.getMatrixId();

    int[] index = genIndexs(feaNum, nnz);
    MatrixMeta meta = worker.getPSAgent().getMatrix(SPARSE_FLOAT_MAT_COMP);

    long blockColNum = meta.getBlockColNum();
    int partNum = (feaNum + (int) blockColNum - 1) / (int) blockColNum;
    IntFloatVector[] subVecs = new IntFloatVector[partNum];
    for (int i = 0; i < partNum; i++) {
      subVecs[i] = VFactory.sparseFloatVector((int) blockColNum, nnz / partNum);
    }

    CompIntFloatVector deltaVec = new CompIntFloatVector(feaNum, subVecs, (int) blockColNum);

    for (int i = 0; i < index.length; i++) {
      deltaVec.set(index[i], index[i]);
    }

    deltaVec.setRowId(0);

    client1.increment(deltaVec);
    client1.clock().get();

    IndexGet func = new IndexGet(new IndexGetParam(matrixW1Id, 0, index));

    CompIntFloatVector row = (CompIntFloatVector) client1.get(0, index);
    for (int id : index) {
      //System.out.println("id=" + id + ", value=" + row.get(id));
      Assert.assertEquals(row.get(id), deltaVec.get(id), 0.000000001);
    }

    Assert.assertTrue(index.length == row.size());
  }


  public void testDenseIntUDF() throws Exception {
    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient client1 = worker.getPSAgent().getMatrixClient(DENSE_INT_MAT, 0);
    int matrixW1Id = client1.getMatrixId();

    int[] index = genIndexs(feaNum, nnz);

    IntIntVector deltaVec = new IntIntVector(feaNum, new IntIntDenseVectorStorage(feaNum));
    for (int i = 0; i < feaNum; i++)
      deltaVec.set(i, i);
    deltaVec.setRowId(0);

    client1.increment(deltaVec);
    client1.clock().get();

    //IndexGet func = new IndexGet(new IndexGetParam(matrixW1Id, 0, index));
    IntIntVector row = (IntIntVector) client1.get(0, index);
    for (int id : index) {
      Assert.assertTrue(row.get(id) == deltaVec.get(id));
    }
    Assert.assertTrue(index.length == row.size());

  }

  public void testSparseIntUDF() throws Exception {

    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient client1 = worker.getPSAgent().getMatrixClient(SPARSE_INT_MAT, 0);
    int matrixW1Id = client1.getMatrixId();

    int[] index = genIndexs(feaNum, nnz);

    IntIntVector deltaVec = new IntIntVector(feaNum, new IntIntSparseVectorStorage(feaNum, nnz));
    for (int i = 0; i < index.length; i++) {
      deltaVec.set(index[i], index[i]);
    }
    //for (int i = 0; i < feaNum; i++) {
    //  deltaVec.set(i, i);
    //}
    deltaVec.setRowId(0);

    client1.increment(deltaVec);
    client1.clock().get();

    //IndexGet func = new IndexGet(new IndexGetParam(matrixW1Id, 0, index));

    IntIntVector row = (IntIntVector) client1.get(0, index);
    for (int id : index) {
      //System.out.println("id=" + id + ", value=" + row.get(id));
      Assert.assertTrue(row.get(id) == deltaVec.get(id));
    }

    Assert.assertTrue(index.length == row.size());
  }

  public void testDenseIntCompUDF() throws Exception {
    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient client1 = worker.getPSAgent().getMatrixClient(DENSE_INT_MAT_COMP, 0);
    int matrixW1Id = client1.getMatrixId();

    int[] index = genIndexs(feaNum, nnz);

    MatrixMeta meta = worker.getPSAgent().getMatrix(DENSE_INT_MAT_COMP);

    long blockColNum = meta.getBlockColNum();
    int partNum = (feaNum + (int) blockColNum - 1) / (int) blockColNum;
    IntIntVector[] subVecs = new IntIntVector[partNum];
    for (int i = 0; i < partNum; i++) {
      subVecs[i] = VFactory.denseIntVector((int) blockColNum);
    }

    CompIntIntVector deltaVec = new CompIntIntVector(feaNum, subVecs, (int) blockColNum);

    for (int i = 0; i < feaNum; i++)
      deltaVec.set(i, i);
    deltaVec.setRowId(0);

    client1.increment(deltaVec);
    client1.clock().get();

    IndexGet func = new IndexGet(new IndexGetParam(matrixW1Id, 0, index));
    CompIntIntVector row = (CompIntIntVector) client1.get(0, index);
    for (int id : index) {
      Assert.assertTrue(row.get(id) == deltaVec.get(id));
    }
    Assert.assertTrue(index.length == row.size());

  }

  public void testDenseLongCompUDF() throws Exception {
    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient client1 = worker.getPSAgent().getMatrixClient(DENSE_LONG_MAT_COMP, 0);
    int matrixW1Id = client1.getMatrixId();

    int[] index = genIndexs(feaNum, nnz);

    MatrixMeta meta = worker.getPSAgent().getMatrix(DENSE_LONG_MAT_COMP);

    long blockColNum = meta.getBlockColNum();
    int partNum = (feaNum + (int) blockColNum - 1) / (int) blockColNum;
    IntLongVector[] subVecs = new IntLongVector[partNum];
    for (int i = 0; i < partNum; i++) {
      subVecs[i] = VFactory.denseLongVector((int) blockColNum);
    }

    CompIntLongVector deltaVec = new CompIntLongVector(feaNum, subVecs, (int) blockColNum);

    for (int i = 0; i < feaNum; i++)
      deltaVec.set(i, i);
    deltaVec.setRowId(0);

    client1.increment(deltaVec);
    client1.clock().get();

    IndexGet func = new IndexGet(new IndexGetParam(matrixW1Id, 0, index));
    CompIntLongVector row = (CompIntLongVector) client1.get(0, index);
    for (int id : index) {
      Assert.assertTrue(row.get(id) == deltaVec.get(id));
    }
    Assert.assertTrue(index.length == row.size());

  }

  public void testSparseLongCompUDF() throws Exception {
    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient client1 = worker.getPSAgent().getMatrixClient(SPARSE_LONG_MAT_COMP, 0);
    int matrixW1Id = client1.getMatrixId();

    int[] index = genIndexs(feaNum, nnz);

    MatrixMeta meta = worker.getPSAgent().getMatrix(SPARSE_LONG_MAT_COMP);

    long blockColNum = meta.getBlockColNum();
    int partNum = (feaNum + (int) blockColNum - 1) / (int) blockColNum;
    IntLongVector[] subVecs = new IntLongVector[partNum];
    for (int i = 0; i < partNum; i++) {
      subVecs[i] = VFactory.denseLongVector((int) blockColNum);
    }

    CompIntLongVector deltaVec = new CompIntLongVector(feaNum, subVecs, (int) blockColNum);

    for (int i = 0; i < nnz; i++)
      deltaVec.set(index[i], index[i]);
    deltaVec.setRowId(0);

    client1.increment(deltaVec);
    client1.clock().get();

    IndexGet func = new IndexGet(new IndexGetParam(matrixW1Id, 0, index));
    CompIntLongVector row = (CompIntLongVector) client1.get(0, index);
    for (int id : index) {
      Assert.assertTrue(row.get(id) == deltaVec.get(id));
    }
    Assert.assertTrue(index.length == row.size());

  }

  public void testDenseLongUDF() throws Exception {
    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient client1 = worker.getPSAgent().getMatrixClient(DENSE_LONG_MAT, 0);
    int matrixW1Id = client1.getMatrixId();

    int[] index = genIndexs(feaNum, nnz);

    IntLongVector deltaVec = new IntLongVector(feaNum, new IntLongDenseVectorStorage(feaNum));
    for (int i = 0; i < feaNum; i++)
      deltaVec.set(i, i);
    deltaVec.setRowId(0);

    client1.increment(deltaVec);
    client1.clock().get();

    //IndexGet func = new IndexGet(new IndexGetParam(matrixW1Id, 0, index));
    IntLongVector row = (IntLongVector) client1.get(0, index);
    for (int id : index) {
      //System.out.println("id=" + id + ", value=" + row.get(id));
      Assert.assertEquals(row.get(id), deltaVec.get(id));
    }
    Assert.assertTrue(index.length == row.size());

  }

  public void testSparseLongUDF() throws Exception {

    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient client1 = worker.getPSAgent().getMatrixClient(SPARSE_LONG_MAT, 0);
    int matrixW1Id = client1.getMatrixId();

    int[] index = genIndexs(feaNum, nnz);

    IntLongVector deltaVec = new IntLongVector(feaNum, new IntLongSparseVectorStorage(feaNum, nnz));
    for (int i = 0; i < index.length; i++) {
      deltaVec.set(index[i], index[i]);
    }
    //for (int i = 0; i < feaNum; i++) {
    //  deltaVec.set(i, i);
    //}
    deltaVec.setRowId(0);

    client1.increment(deltaVec);
    client1.clock().get();

    //IndexGet func = new IndexGet(new IndexGetParam(matrixW1Id, 0, index));

    //IntLongVector row = (IntLongVector) ((GetRowResult) client1.get(func)).getRow();
    IntLongVector row = (IntLongVector) client1.get(0, index);
    for (int id : index) {
      //System.out.println("id=" + id + ", value=" + row.get(id));
      Assert.assertTrue(row.get(id) == deltaVec.get(id));
    }

    Assert.assertTrue(index.length == row.size());
  }

  public void testSparseIntCompUDF() throws Exception {

    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient client1 = worker.getPSAgent().getMatrixClient(SPARSE_INT_MAT_COMP, 0);
    int matrixW1Id = client1.getMatrixId();
    MatrixMeta meta = worker.getPSAgent().getMatrix(SPARSE_INT_MAT_COMP);

    int[] index = genIndexs(feaNum, nnz);

    long blockColNum = meta.getBlockColNum();
    int partNum = (feaNum + (int) blockColNum - 1) / (int) blockColNum;
    IntIntVector[] subVecs = new IntIntVector[partNum];
    for (int i = 0; i < partNum; i++) {
      subVecs[i] = VFactory.sparseIntVector((int) blockColNum, nnz / partNum);
    }

    CompIntIntVector deltaVec = new CompIntIntVector(feaNum, subVecs, (int) blockColNum);
    for (int i = 0; i < nnz; i++)
      deltaVec.set(index[i], index[i]);
    deltaVec.setRowId(0);

    client1.increment(deltaVec);
    client1.clock().get();

    IndexGet func = new IndexGet(new IndexGetParam(matrixW1Id, 0, index));

    CompIntIntVector row = (CompIntIntVector) client1.get(0, index);
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

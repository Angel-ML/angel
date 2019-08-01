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
import com.tencent.angel.ml.math2.storage.*;
import com.tencent.angel.ml.math2.vector.*;
import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.math2.utils.RowType;
import com.tencent.angel.ps.PSAttemptId;
import com.tencent.angel.ps.ParameterServerId;
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

public class UpdateRowsTest {
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

  int feaNum = 100000;
  int nnz = 5;
  int rowNum = 1;
  int blockRowNum = 1;
  int blockColNum = 1000;
  double zero = 0.00000001;

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
    conf.setInt(AngelConf.ANGEL_PS_NUMBER, 2);
    conf.setInt(AngelConf.ANGEL_WORKER_TASK_NUMBER, 1);
    conf.setInt(AngelConf.ANGEL_MODEL_PARTITIONER_PARTITION_SIZE, 1000);
    conf.setBoolean("use.new.split", false);


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
    dMat.setRowNum(rowNum);
    dMat.setColNum(feaNum);
    dMat.setMaxRowNumInBlock(blockRowNum);
    dMat.setMaxColNumInBlock(blockColNum);
    dMat.setRowType(RowType.T_DOUBLE_DENSE);
    angelClient.addMatrix(dMat);

    // add comp dense double matrix
    MatrixContext dcMat = new MatrixContext();
    dcMat.setName(DENSE_DOUBLE_MAT_COMP);
    dcMat.setRowNum(rowNum);
    dcMat.setColNum(feaNum);
    dcMat.setMaxRowNumInBlock(blockRowNum);
    dcMat.setMaxColNumInBlock(blockColNum);
    dcMat.setRowType(RowType.T_DOUBLE_DENSE_COMPONENT);
    angelClient.addMatrix(dcMat);

    // add sparse double matrix
    MatrixContext sMat = new MatrixContext();
    sMat.setName(SPARSE_DOUBLE_MAT);
    sMat.setRowNum(rowNum);
    sMat.setColNum(feaNum);
    sMat.setMaxRowNumInBlock(blockRowNum);
    sMat.setMaxColNumInBlock(blockColNum);
    sMat.setRowType(RowType.T_DOUBLE_SPARSE);
    angelClient.addMatrix(sMat);

    // add component sparse double matrix
    MatrixContext sCompMat = new MatrixContext();
    sCompMat.setName(SPARSE_DOUBLE_MAT_COMP);
    sCompMat.setRowNum(rowNum);
    sCompMat.setColNum(feaNum);
    sCompMat.setMaxRowNumInBlock(blockRowNum);
    sCompMat.setMaxColNumInBlock(blockColNum);
    sCompMat.setRowType(RowType.T_DOUBLE_SPARSE_COMPONENT);
    angelClient.addMatrix(sCompMat);

    // add dense float matrix
    MatrixContext dfMat = new MatrixContext();
    dfMat.setName(DENSE_FLOAT_MAT);
    dfMat.setRowNum(rowNum);
    dfMat.setColNum(feaNum);
    dfMat.setMaxRowNumInBlock(blockRowNum);
    dfMat.setMaxColNumInBlock(blockColNum);
    dfMat.setRowType(RowType.T_FLOAT_DENSE);
    angelClient.addMatrix(dfMat);

    // add comp dense float matrix
    MatrixContext dcfMat = new MatrixContext();
    dcfMat.setName(DENSE_FLOAT_MAT_COMP);
    dcfMat.setRowNum(rowNum);
    dcfMat.setColNum(feaNum);
    dcfMat.setMaxRowNumInBlock(blockRowNum);
    dcfMat.setMaxColNumInBlock(blockColNum);
    dcfMat.setRowType(RowType.T_FLOAT_DENSE_COMPONENT);
    angelClient.addMatrix(dcfMat);

    // add sparse float matrix
    MatrixContext sfMat = new MatrixContext();
    sfMat.setName(SPARSE_FLOAT_MAT);
    sfMat.setRowNum(rowNum);
    sfMat.setColNum(feaNum);
    sfMat.setMaxRowNumInBlock(blockRowNum);
    sfMat.setMaxColNumInBlock(blockColNum);
    sfMat.setRowType(RowType.T_FLOAT_SPARSE);
    angelClient.addMatrix(sfMat);

    // add component sparse float matrix
    MatrixContext sfCompMat = new MatrixContext();
    sfCompMat.setName(SPARSE_FLOAT_MAT_COMP);
    sfCompMat.setRowNum(rowNum);
    sfCompMat.setColNum(feaNum);
    sfCompMat.setMaxRowNumInBlock(blockRowNum);
    sfCompMat.setMaxColNumInBlock(blockColNum);
    sfCompMat.setRowType(RowType.T_FLOAT_SPARSE_COMPONENT);
    angelClient.addMatrix(sfCompMat);

    // add dense float matrix
    MatrixContext diMat = new MatrixContext();
    diMat.setName(DENSE_INT_MAT);
    diMat.setRowNum(rowNum);
    diMat.setColNum(feaNum);
    diMat.setMaxRowNumInBlock(blockRowNum);
    diMat.setMaxColNumInBlock(blockColNum);
    diMat.setRowType(RowType.T_INT_DENSE);
    angelClient.addMatrix(diMat);

    // add comp dense float matrix
    MatrixContext dciMat = new MatrixContext();
    dciMat.setName(DENSE_INT_MAT_COMP);
    dciMat.setRowNum(rowNum);
    dciMat.setColNum(feaNum);
    dciMat.setMaxRowNumInBlock(blockRowNum);
    dciMat.setMaxColNumInBlock(blockColNum);
    dciMat.setRowType(RowType.T_INT_DENSE_COMPONENT);
    angelClient.addMatrix(dciMat);

    // add sparse float matrix
    MatrixContext siMat = new MatrixContext();
    siMat.setName(SPARSE_INT_MAT);
    siMat.setRowNum(rowNum);
    siMat.setColNum(feaNum);
    siMat.setMaxRowNumInBlock(blockRowNum);
    siMat.setMaxColNumInBlock(blockColNum);
    siMat.setRowType(RowType.T_INT_SPARSE);
    angelClient.addMatrix(siMat);

    // add component sparse float matrix
    MatrixContext siCompMat = new MatrixContext();
    siCompMat.setName(SPARSE_INT_MAT_COMP);
    siCompMat.setRowNum(rowNum);
    siCompMat.setColNum(feaNum);
    siCompMat.setMaxRowNumInBlock(blockRowNum);
    siCompMat.setMaxColNumInBlock(blockColNum);
    siCompMat.setRowType(RowType.T_INT_SPARSE_COMPONENT);
    angelClient.addMatrix(siCompMat);

    // add dense long matrix
    MatrixContext dlMat = new MatrixContext();
    dlMat.setName(DENSE_LONG_MAT);
    dlMat.setRowNum(rowNum);
    dlMat.setColNum(feaNum);
    dlMat.setMaxRowNumInBlock(blockRowNum);
    dlMat.setMaxColNumInBlock(blockColNum);
    dlMat.setRowType(RowType.T_LONG_DENSE);
    angelClient.addMatrix(dlMat);

    // add comp dense long matrix
    MatrixContext dclMat = new MatrixContext();
    dclMat.setName(DENSE_LONG_MAT_COMP);
    dclMat.setRowNum(rowNum);
    dclMat.setColNum(feaNum);
    dclMat.setMaxRowNumInBlock(blockRowNum);
    dclMat.setMaxColNumInBlock(blockColNum);
    dclMat.setRowType(RowType.T_LONG_DENSE_COMPONENT);
    angelClient.addMatrix(dclMat);

    // add sparse long matrix
    MatrixContext slMat = new MatrixContext();
    slMat.setName(SPARSE_LONG_MAT);
    slMat.setRowNum(rowNum);
    slMat.setColNum(feaNum);
    slMat.setMaxRowNumInBlock(blockRowNum);
    slMat.setMaxColNumInBlock(blockColNum);
    slMat.setRowType(RowType.T_LONG_SPARSE);
    angelClient.addMatrix(slMat);

    // add component sparse long matrix
    MatrixContext slcMat = new MatrixContext();
    slcMat.setName(SPARSE_LONG_MAT_COMP);
    slcMat.setRowNum(rowNum);
    slcMat.setColNum(feaNum);
    slcMat.setMaxRowNumInBlock(blockRowNum);
    slcMat.setMaxColNumInBlock(blockColNum);
    slcMat.setRowType(RowType.T_LONG_SPARSE_COMPONENT);
    angelClient.addMatrix(slcMat);

    // add comp dense long double matrix
    MatrixContext dldcMatrix = new MatrixContext();
    dldcMatrix.setName(DENSE_DOUBLE_LONG_MAT_COMP);
    dldcMatrix.setRowNum(rowNum);
    dldcMatrix.setColNum(feaNum);
    dldcMatrix.setMaxRowNumInBlock(blockRowNum);
    dldcMatrix.setMaxColNumInBlock(blockColNum);
    dldcMatrix.setRowType(RowType.T_DOUBLE_DENSE_LONGKEY_COMPONENT);
    angelClient.addMatrix(dldcMatrix);

    // add sparse long-key double matrix
    MatrixContext dLongKeysMatrix = new MatrixContext();
    dLongKeysMatrix.setName(SPARSE_DOUBLE_LONG_MAT);
    dLongKeysMatrix.setRowNum(rowNum);
    dLongKeysMatrix.setColNum(feaNum);
    dLongKeysMatrix.setMaxRowNumInBlock(blockRowNum);
    dLongKeysMatrix.setMaxColNumInBlock(blockColNum);
    dLongKeysMatrix.setRowType(RowType.T_DOUBLE_SPARSE_LONGKEY);
    angelClient.addMatrix(dLongKeysMatrix);

    // add component long-key sparse double matrix
    MatrixContext dLongKeysCompMatrix = new MatrixContext();
    dLongKeysCompMatrix.setName(SPARSE_DOUBLE_LONG_MAT_COMP);
    dLongKeysCompMatrix.setRowNum(rowNum);
    dLongKeysCompMatrix.setColNum(feaNum);
    dLongKeysCompMatrix.setMaxRowNumInBlock(blockRowNum);
    dLongKeysCompMatrix.setMaxColNumInBlock(blockColNum);
    dLongKeysCompMatrix.setRowType(RowType.T_DOUBLE_SPARSE_LONGKEY_COMPONENT);
    angelClient.addMatrix(dLongKeysCompMatrix);

    // add component long-key sparse float matrix
    MatrixContext dlfcMatrix = new MatrixContext();
    dlfcMatrix.setName(DENSE_FLOAT_LONG_MAT_COMP);
    dlfcMatrix.setRowNum(rowNum);
    dlfcMatrix.setColNum(feaNum);
    dlfcMatrix.setMaxRowNumInBlock(blockRowNum);
    dlfcMatrix.setMaxColNumInBlock(blockColNum);
    dlfcMatrix.setRowType(RowType.T_FLOAT_DENSE_LONGKEY_COMPONENT);
    angelClient.addMatrix(dlfcMatrix);

    // add sparse long-key float matrix
    MatrixContext slfMatrix = new MatrixContext();
    slfMatrix.setName(SPARSE_FLOAT_LONG_MAT);
    slfMatrix.setRowNum(rowNum);
    slfMatrix.setColNum(feaNum);
    slfMatrix.setMaxRowNumInBlock(blockRowNum);
    slfMatrix.setMaxColNumInBlock(blockColNum);
    slfMatrix.setRowType(RowType.T_FLOAT_SPARSE_LONGKEY);
    angelClient.addMatrix(slfMatrix);

    // add component long-key sparse float matrix
    MatrixContext slfcMatrix = new MatrixContext();
    slfcMatrix.setName(SPARSE_FLOAT_LONG_MAT_COMP);
    slfcMatrix.setRowNum(rowNum);
    slfcMatrix.setColNum(feaNum);
    slfcMatrix.setMaxRowNumInBlock(blockRowNum);
    slfcMatrix.setMaxColNumInBlock(blockColNum);
    slfcMatrix.setRowType(RowType.T_FLOAT_SPARSE_LONGKEY_COMPONENT);
    angelClient.addMatrix(slfcMatrix);

    // add component long-key sparse int matrix
    MatrixContext dlicMatrix = new MatrixContext();
    dlicMatrix.setName(DENSE_INT_LONG_MAT_COMP);
    dlicMatrix.setRowNum(rowNum);
    dlicMatrix.setColNum(feaNum);
    dlicMatrix.setMaxRowNumInBlock(blockRowNum);
    dlicMatrix.setMaxColNumInBlock(blockColNum);
    dlicMatrix.setRowType(RowType.T_INT_DENSE_LONGKEY_COMPONENT);
    angelClient.addMatrix(dlicMatrix);

    // add sparse long-key int matrix
    MatrixContext sliMatrix = new MatrixContext();
    sliMatrix.setName(SPARSE_INT_LONG_MAT);
    sliMatrix.setRowNum(rowNum);
    sliMatrix.setColNum(feaNum);
    sliMatrix.setMaxRowNumInBlock(blockRowNum);
    sliMatrix.setMaxColNumInBlock(blockColNum);
    sliMatrix.setRowType(RowType.T_INT_SPARSE_LONGKEY);
    angelClient.addMatrix(sliMatrix);

    // add component long-key sparse int matrix
    MatrixContext slicMatrix = new MatrixContext();
    slicMatrix.setName(SPARSE_INT_LONG_MAT_COMP);
    slicMatrix.setRowNum(rowNum);
    slicMatrix.setColNum(feaNum);
    slicMatrix.setMaxRowNumInBlock(blockRowNum);
    slicMatrix.setMaxColNumInBlock(blockColNum);
    slicMatrix.setRowType(RowType.T_INT_SPARSE_LONGKEY_COMPONENT);
    angelClient.addMatrix(slicMatrix);

    // add component long-key sparse long matrix
    MatrixContext dllcMatrix = new MatrixContext();
    dllcMatrix.setName(DENSE_LONG_LONG_MAT_COMP);
    dllcMatrix.setRowNum(rowNum);
    dllcMatrix.setColNum(feaNum);
    dllcMatrix.setMaxRowNumInBlock(blockRowNum);
    dllcMatrix.setMaxColNumInBlock(blockColNum);
    dllcMatrix.setRowType(RowType.T_LONG_DENSE_LONGKEY_COMPONENT);
    angelClient.addMatrix(dllcMatrix);

    // add sparse long-key long matrix
    MatrixContext sllMatrix = new MatrixContext();
    sllMatrix.setName(SPARSE_LONG_LONG_MAT);
    sllMatrix.setRowNum(rowNum);
    sllMatrix.setColNum(feaNum);
    sllMatrix.setMaxRowNumInBlock(blockRowNum);
    sllMatrix.setMaxColNumInBlock(blockColNum);
    sllMatrix.setRowType(RowType.T_LONG_SPARSE_LONGKEY);
    angelClient.addMatrix(sllMatrix);

    // add component long-key sparse long matrix
    MatrixContext sllcMatrix = new MatrixContext();
    sllcMatrix.setName(SPARSE_LONG_LONG_MAT_COMP);
    sllcMatrix.setRowNum(rowNum);
    sllcMatrix.setColNum(feaNum);
    sllcMatrix.setMaxRowNumInBlock(blockRowNum);
    sllcMatrix.setMaxColNumInBlock(blockColNum);
    sllcMatrix.setRowType(RowType.T_LONG_SPARSE_LONGKEY_COMPONENT);
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

    LongDoubleVector deltaVec = null;
    for (int rowId = 0; rowId < rowNum; rowId++) {
      deltaVec = new LongDoubleVector(feaNum, new LongDoubleSparseVectorStorage(feaNum));
      for (int i = 0; i < index.length; i++) {
        deltaVec.set(index[i], index[i]);
      }
      client1.update(rowId, deltaVec);
    }

    int[] rowIds = new int[rowNum];
    for (int i = 0; i < rowNum; i++) {
      rowIds[i] = i;
    }

    Vector[] rows = client1.get(rowIds, index);
    for (int i = 0; i < rowNum; i++) {
      for (long id : index) {
        Assert.assertEquals(((LongDoubleVector) rows[i]).get(id), deltaVec.get(id), zero);
      }
    }

    client1.zero();

    LongDoubleVector[] deltaVecs = new LongDoubleVector[rowNum];
    for (int rowId = 0; rowId < rowNum; rowId++) {
      deltaVec = new LongDoubleVector(feaNum, new LongDoubleSparseVectorStorage(feaNum));
      for (int i = 0; i < index.length; i++) {
        deltaVec.set(index[i], index[i]);
      }
      deltaVecs[rowId] = deltaVec;
    }

    client1.update(rowIds, deltaVecs);

    rows = client1.get(rowIds, index);
    for (int i = 0; i < rowNum; i++) {
      for (long id : index) {
        Assert.assertEquals(((LongDoubleVector) rows[i]).get(id), deltaVecs[i].get(id), zero);
      }
    }

    client1.zero();

    deltaVecs = new LongDoubleVector[rowNum];
    for (int rowId = 0; rowId < rowNum; rowId++) {
      deltaVec = new LongDoubleVector(feaNum, new LongDoubleSparseVectorStorage(feaNum));
      for (int i = 0; i < index.length; i++) {
        deltaVec.set(index[i], index[i]);
      }
      deltaVecs[rowId] = deltaVec;
    }

    client1.increment(rowIds, deltaVecs, true);

    rows = client1.get(rowIds, index);
    for (int i = 0; i < rowNum; i++) {
      for (long id : index) {
        Assert.assertEquals(((LongDoubleVector) rows[i]).get(id), deltaVecs[i].get(id), zero);
      }
    }
  }


  public void testSparseDoubleLongKeyCompUDF() throws Exception {
    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient client1 = worker.getPSAgent().getMatrixClient(SPARSE_DOUBLE_LONG_MAT_COMP, 0);
    int matrixW1Id = client1.getMatrixId();
    long blockColNum =
      worker.getPSAgent().getMatrixMetaManager().getMatrixMeta(matrixW1Id).getBlockColNum();

    long[] index = genLongIndexs(feaNum, nnz);

    CompLongDoubleVector deltaVec = null;
    for (int rowId = 0; rowId < rowNum; rowId++) {
      int partNum = (feaNum + (int) blockColNum - 1) / (int) blockColNum;
      LongDoubleVector[] subVecs = new LongDoubleVector[partNum];
      for (int i = 0; i < partNum; i++) {
        subVecs[i] = VFactory.sparseLongKeyDoubleVector((int) blockColNum);
      }

      deltaVec = new CompLongDoubleVector(feaNum, subVecs, (int) blockColNum);
      for (int i = 0; i < nnz; i++)
        deltaVec.set(index[i], index[i]);

      client1.update(rowId, deltaVec);
    }

    int[] rowIds = new int[rowNum];
    for (int i = 0; i < rowNum; i++) {
      rowIds[i] = i;
    }

    Vector[] rows = client1.get(rowIds, index);
    for (int i = 0; i < rowNum; i++) {
      for (long id : index) {
        LOG.info(
          "rows[" + i + "][" + id + "]=" + ((CompLongDoubleVector) rows[i]).get(id) + ", delta["
            + id + "]=" + deltaVec.get(id));
        Assert.assertEquals(((CompLongDoubleVector) rows[i]).get(id), deltaVec.get(id), zero);
      }
    }
  }

  public void testSparseFloatLongKeyUDF() throws Exception {

    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient client1 = worker.getPSAgent().getMatrixClient(SPARSE_FLOAT_LONG_MAT, 0);
    int matrixW1Id = client1.getMatrixId();

    long[] index = genLongIndexs(feaNum, nnz);


    LongFloatVector deltaVec = null;
    for (int rowId = 0; rowId < rowNum; rowId++) {
      deltaVec = new LongFloatVector(feaNum, new LongFloatSparseVectorStorage(feaNum));
      for (int i = 0; i < index.length; i++) {
        deltaVec.set(index[i], index[i]);
      }
      client1.update(rowId, deltaVec);
    }

    int[] rowIds = new int[rowNum];
    for (int i = 0; i < rowNum; i++) {
      rowIds[i] = i;
    }

    Vector[] rows = client1.get(rowIds, index);
    for (int i = 0; i < rowNum; i++) {
      for (long id : index) {
        Assert.assertEquals(((LongFloatVector) rows[i]).get(id), deltaVec.get(id), zero);
      }
    }

    client1.zero();

    LongFloatVector[] deltaVecs = new LongFloatVector[rowNum];
    for (int rowId = 0; rowId < rowNum; rowId++) {
      deltaVec = VFactory.sparseLongKeyFloatVector(feaNum, index.length);
      for (int i = 0; i < index.length; i++) {
        deltaVec.set(index[i], index[i]);
      }
      deltaVecs[rowId] = deltaVec;
    }

    client1.update(rowIds, deltaVecs);

    rows = client1.get(rowIds, index);
    for (int i = 0; i < rowNum; i++) {
      for (long id : index) {
        Assert.assertEquals(((LongFloatVector) rows[i]).get(id), deltaVecs[i].get(id), zero);
      }
    }

    client1.zero();

    deltaVecs = new LongFloatVector[rowNum];
    for (int rowId = 0; rowId < rowNum; rowId++) {
      deltaVec = VFactory.sparseLongKeyFloatVector(feaNum, index.length);
      for (int i = 0; i < index.length; i++) {
        deltaVec.set(index[i], index[i]);
      }
      deltaVecs[rowId] = deltaVec;
    }

    client1.increment(rowIds, deltaVecs, true);

    rows = client1.get(rowIds, index);
    for (int i = 0; i < rowNum; i++) {
      for (long id : index) {
        Assert.assertEquals(((LongFloatVector) rows[i]).get(id), deltaVecs[i].get(id), zero);
      }
    }
  }

  public void testSparseFloatLongKeyCompUDF() throws Exception {
    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient client1 = worker.getPSAgent().getMatrixClient(SPARSE_FLOAT_LONG_MAT_COMP, 0);
    int matrixW1Id = client1.getMatrixId();
    long blockColNum =
      worker.getPSAgent().getMatrixMetaManager().getMatrixMeta(matrixW1Id).getBlockColNum();

    long[] index = genLongIndexs(feaNum, nnz);

    CompLongFloatVector deltaVec = null;
    for (int rowId = 0; rowId < rowNum; rowId++) {
      int partNum = (feaNum + (int) blockColNum - 1) / (int) blockColNum;
      LongFloatVector[] subVecs = new LongFloatVector[partNum];
      for (int i = 0; i < partNum; i++) {
        subVecs[i] = VFactory.sparseLongKeyFloatVector((int) blockColNum);
      }

      deltaVec = new CompLongFloatVector(feaNum, subVecs, (int) blockColNum);
      for (int i = 0; i < nnz; i++)
        deltaVec.set(index[i], index[i]);

      client1.update(rowId, deltaVec);
    }

    int[] rowIds = new int[rowNum];
    for (int i = 0; i < rowNum; i++) {
      rowIds[i] = i;
    }

    Vector[] rows = client1.get(rowIds, index);
    for (int i = 0; i < rowNum; i++) {
      for (long id : index) {
        Assert.assertEquals(((CompLongFloatVector) rows[i]).get(id), deltaVec.get(id), zero);
      }
    }
  }

  public void testSparseLongLongKeyUDF() throws Exception {

    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient client1 = worker.getPSAgent().getMatrixClient(SPARSE_LONG_LONG_MAT, 0);
    int matrixW1Id = client1.getMatrixId();

    long[] index = genLongIndexs(feaNum, nnz);

    LongLongVector deltaVec = null;
    for (int rowId = 0; rowId < rowNum; rowId++) {
      deltaVec = new LongLongVector(feaNum, new LongLongSparseVectorStorage(feaNum));
      for (int i = 0; i < index.length; i++) {
        deltaVec.set(index[i], index[i]);
      }
      client1.update(rowId, deltaVec);
    }

    int[] rowIds = new int[rowNum];
    for (int i = 0; i < rowNum; i++) {
      rowIds[i] = i;
    }

    Vector[] rows = client1.get(rowIds, index);
    for (int i = 0; i < rowNum; i++) {
      for (long id : index) {
        Assert.assertEquals(((LongLongVector) rows[i]).get(id), deltaVec.get(id), zero);
      }
    }

    client1.zero();

    LongLongVector[] deltaVecs = new LongLongVector[rowNum];
    for (int rowId = 0; rowId < rowNum; rowId++) {
      deltaVec = VFactory.sparseLongKeyLongVector(feaNum, index.length);
      for (int i = 0; i < index.length; i++) {
        deltaVec.set(index[i], index[i]);
      }
      deltaVecs[rowId] = deltaVec;
    }

    client1.update(rowIds, deltaVecs);

    rows = client1.get(rowIds, index);
    for (int i = 0; i < rowNum; i++) {
      for (long id : index) {
        Assert.assertEquals(((LongLongVector) rows[i]).get(id), deltaVecs[i].get(id), zero);
      }
    }

    client1.zero();

    deltaVecs = new LongLongVector[rowNum];
    for (int rowId = 0; rowId < rowNum; rowId++) {
      deltaVec = VFactory.sparseLongKeyLongVector(feaNum, index.length);
      for (int i = 0; i < index.length; i++) {
        deltaVec.set(index[i], index[i]);
      }
      deltaVecs[rowId] = deltaVec;
    }

    client1.increment(rowIds, deltaVecs, true);

    rows = client1.get(rowIds, index);
    for (int i = 0; i < rowNum; i++) {
      for (long id : index) {
        Assert.assertEquals(((LongLongVector) rows[i]).get(id), deltaVecs[i].get(id), zero);
      }
    }
  }

  public void testSparseLongLongKeyCompUDF() throws Exception {
    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient client1 = worker.getPSAgent().getMatrixClient(SPARSE_LONG_LONG_MAT_COMP, 0);
    int matrixW1Id = client1.getMatrixId();
    long blockColNum =
      worker.getPSAgent().getMatrixMetaManager().getMatrixMeta(matrixW1Id).getBlockColNum();

    long[] index = genLongIndexs(feaNum, nnz);

    CompLongLongVector deltaVec = null;
    for (int rowId = 0; rowId < rowNum; rowId++) {
      int partNum = (feaNum + (int) blockColNum - 1) / (int) blockColNum;
      LongLongVector[] subVecs = new LongLongVector[partNum];
      for (int i = 0; i < partNum; i++) {
        subVecs[i] = VFactory.sparseLongKeyLongVector((int) blockColNum);
      }

      deltaVec = new CompLongLongVector(feaNum, subVecs, (int) blockColNum);
      for (int i = 0; i < nnz; i++)
        deltaVec.set(index[i], index[i]);

      client1.update(rowId, deltaVec);
    }

    int[] rowIds = new int[rowNum];
    for (int i = 0; i < rowNum; i++) {
      rowIds[i] = i;
    }

    Vector[] rows = client1.get(rowIds, index);
    for (int i = 0; i < rowNum; i++) {
      for (long id : index) {
        Assert.assertEquals(((CompLongLongVector) rows[i]).get(id), deltaVec.get(id), zero);
      }
    }
  }

  public void testSparseIntLongKeyUDF() throws Exception {

    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient client1 = worker.getPSAgent().getMatrixClient(SPARSE_INT_LONG_MAT, 0);
    int matrixW1Id = client1.getMatrixId();

    long[] index = genLongIndexs(feaNum, nnz);

    LongIntVector deltaVec = null;
    for (int rowId = 0; rowId < rowNum; rowId++) {
      deltaVec = new LongIntVector(feaNum, new LongIntSparseVectorStorage(feaNum));
      for (int i = 0; i < index.length; i++) {
        deltaVec.set(index[i], (int) index[i]);
      }
      client1.update(rowId, deltaVec);
    }

    int[] rowIds = new int[rowNum];
    for (int i = 0; i < rowNum; i++) {
      rowIds[i] = i;
    }

    Vector[] rows = client1.get(rowIds, index);
    for (int i = 0; i < rowNum; i++) {
      for (long id : index) {
        Assert.assertEquals(((LongIntVector) rows[i]).get(id), deltaVec.get(id), zero);
      }
    }

    client1.zero();

    LongIntVector[] deltaVecs = new LongIntVector[rowNum];
    for (int rowId = 0; rowId < rowNum; rowId++) {
      deltaVec = VFactory.sparseLongKeyIntVector(feaNum, index.length);
      for (int i = 0; i < index.length; i++) {
        deltaVec.set(index[i], (int) index[i]);
      }
      deltaVecs[rowId] = deltaVec;
    }

    client1.update(rowIds, deltaVecs);

    rows = client1.get(rowIds, index);
    for (int i = 0; i < rowNum; i++) {
      for (long id : index) {
        Assert.assertEquals(((LongIntVector) rows[i]).get(id), deltaVecs[i].get(id), zero);
      }
    }

    client1.zero();

    deltaVecs = new LongIntVector[rowNum];
    for (int rowId = 0; rowId < rowNum; rowId++) {
      deltaVec = VFactory.sparseLongKeyIntVector(feaNum, index.length);
      for (int i = 0; i < index.length; i++) {
        deltaVec.set(index[i], (int) index[i]);
      }
      deltaVecs[rowId] = deltaVec;
    }

    client1.increment(rowIds, deltaVecs, true);

    rows = client1.get(rowIds, index);
    for (int i = 0; i < rowNum; i++) {
      for (long id : index) {
        Assert.assertEquals(((LongIntVector) rows[i]).get(id), deltaVecs[i].get(id), zero);
      }
    }
  }

  public void testSparseIntLongKeyCompUDF() throws Exception {
    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient client1 = worker.getPSAgent().getMatrixClient(SPARSE_INT_LONG_MAT_COMP, 0);
    int matrixW1Id = client1.getMatrixId();
    long blockColNum =
      worker.getPSAgent().getMatrixMetaManager().getMatrixMeta(matrixW1Id).getBlockColNum();

    long[] index = genLongIndexs(feaNum, nnz);

    CompLongIntVector deltaVec = null;
    for (int rowId = 0; rowId < rowNum; rowId++) {
      int partNum = (feaNum + (int) blockColNum - 1) / (int) blockColNum;
      LongIntVector[] subVecs = new LongIntVector[partNum];
      for (int i = 0; i < partNum; i++) {
        subVecs[i] = VFactory.sparseLongKeyIntVector((int) blockColNum);
      }

      deltaVec = new CompLongIntVector(feaNum, subVecs, (int) blockColNum);
      for (int i = 0; i < nnz; i++)
        deltaVec.set(index[i], (int) index[i]);

      client1.update(rowId, deltaVec);
    }

    int[] rowIds = new int[rowNum];
    for (int i = 0; i < rowNum; i++) {
      rowIds[i] = i;
    }

    Vector[] rows = client1.get(rowIds, index);
    for (int i = 0; i < rowNum; i++) {
      for (long id : index) {
        Assert.assertEquals(((CompLongIntVector) rows[i]).get(id), deltaVec.get(id), zero);
      }
    }
  }


  public void testDenseDoubleUDF() throws Exception {
    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient client1 = worker.getPSAgent().getMatrixClient(DENSE_DOUBLE_MAT, 0);

    int[] index = genIndexs(feaNum, nnz);
    IntDoubleVector deltaVec = null;
    for (int rowId = 0; rowId < rowNum; rowId++) {
      deltaVec = new IntDoubleVector(feaNum, new IntDoubleDenseVectorStorage(feaNum));
      for (int i = 0; i < feaNum; i++)
        deltaVec.set(i, i);
      client1.update(rowId, deltaVec);
    }

    int[] rowIds = new int[rowNum];
    for (int i = 0; i < rowNum; i++) {
      rowIds[i] = i;
    }

    Vector[] rows = client1.get(rowIds, index);
    for (int i = 0; i < rowNum; i++) {
      for (int id : index) {
        Assert.assertEquals(((IntDoubleVector) rows[i]).get(id), deltaVec.get(id), zero);
      }
    }

    client1.zero();

    IntDoubleVector[] deltaVecs = new IntDoubleVector[rowNum];
    for (int rowId = 0; rowId < rowNum; rowId++) {
      deltaVec = new IntDoubleVector(feaNum, new IntDoubleSparseVectorStorage(feaNum));
      for (int i = 0; i < index.length; i++) {
        deltaVec.set(index[i], index[i]);
      }
      deltaVecs[rowId] = deltaVec;
    }

    client1.update(rowIds, deltaVecs);

    rows = client1.get(rowIds, index);
    for (int i = 0; i < rowNum; i++) {
      for (int id : index) {
        Assert.assertEquals(((IntDoubleVector) rows[i]).get(id), deltaVecs[i].get(id), zero);
      }
    }

    client1.zero();

    for (int rowId = 0; rowId < rowNum; rowId++) {
      deltaVec = new IntDoubleVector(feaNum, new IntDoubleSparseVectorStorage(feaNum));
      for (int i = 0; i < index.length; i++) {
        deltaVec.set(index[i], index[i]);
      }
      deltaVecs[rowId] = deltaVec;
    }

    client1.increment(rowIds, deltaVecs, true);

    rows = client1.get(rowIds, index);
    for (int i = 0; i < rowNum; i++) {
      for (int id : index) {
        Assert.assertEquals(((IntDoubleVector) rows[i]).get(id), deltaVecs[i].get(id), zero);
      }
    }
  }

  public void testSparseDoubleUDF() throws Exception {
    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient client1 = worker.getPSAgent().getMatrixClient(SPARSE_DOUBLE_MAT, 0);

    int[] index = genIndexs(feaNum, nnz);
    IntDoubleVector deltaVec = null;
    for (int rowId = 0; rowId < rowNum; rowId++) {
      deltaVec = new IntDoubleVector(feaNum, new IntDoubleSparseVectorStorage(feaNum));
      for (int i = 0; i < index.length; i++) {
        deltaVec.set(index[i], index[i]);
      }
      client1.update(rowId, deltaVec);
    }

    int[] rowIds = new int[rowNum];
    for (int i = 0; i < rowNum; i++) {
      rowIds[i] = i;
    }

    Vector[] rows = client1.get(rowIds, index);
    for (int i = 0; i < rowNum; i++) {
      for (int id : index) {
        Assert.assertEquals(((IntDoubleVector) rows[i]).get(id), deltaVec.get(id), zero);
      }
    }

    client1.zero();
    IntDoubleVector[] deltaVecs = new IntDoubleVector[rowNum];
    for (int rowId = 0; rowId < rowNum; rowId++) {
      deltaVec = VFactory.sparseDoubleVector(feaNum, index.length);
      for (int i = 0; i < index.length; i++) {
        deltaVec.set(index[i], index[i]);
      }
      deltaVecs[rowId] = deltaVec;
    }

    client1.update(rowIds, deltaVecs);

    rows = client1.get(rowIds, index);
    for (int i = 0; i < rowNum; i++) {
      for (int id : index) {
        Assert.assertEquals(((IntDoubleVector) rows[i]).get(id), deltaVecs[i].get(id), zero);
      }
    }

    client1.zero();

    for (int rowId = 0; rowId < rowNum; rowId++) {
      deltaVec = new IntDoubleVector(feaNum, new IntDoubleSparseVectorStorage(feaNum));
      for (int i = 0; i < index.length; i++) {
        deltaVec.set(index[i], index[i]);
      }
      deltaVecs[rowId] = deltaVec;
    }

    client1.increment(rowIds, deltaVecs, true);

    rows = client1.get(rowIds, index);
    for (int i = 0; i < rowNum; i++) {
      for (int id : index) {
        Assert.assertEquals(((IntDoubleVector) rows[i]).get(id), deltaVecs[i].get(id), zero);
      }
    }
  }

  public void testDenseDoubleCompUDF() throws Exception {
    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient client1 = worker.getPSAgent().getMatrixClient(DENSE_DOUBLE_MAT_COMP, 0);
    int matrixW1Id = client1.getMatrixId();

    int[] index = genIndexs(feaNum, nnz);

    CompIntDoubleVector deltaVec = null;
    for (int rowId = 0; rowId < rowNum; rowId++) {
      long blockColNum =
        worker.getPSAgent().getMatrixMetaManager().getMatrixMeta(matrixW1Id).getBlockColNum();
      int partNum = (feaNum + (int) blockColNum - 1) / (int) blockColNum;
      IntDoubleVector[] subVecs = new IntDoubleVector[partNum];
      for (int i = 0; i < partNum; i++) {
        subVecs[i] = VFactory.denseDoubleVector((int) blockColNum);
      }

      deltaVec = new CompIntDoubleVector(feaNum, subVecs, (int) blockColNum);
      for (int i = 0; i < feaNum; i++)
        deltaVec.set(i, i);

      client1.update(rowId, deltaVec);
    }

    int[] rowIds = new int[rowNum];
    for (int i = 0; i < rowNum; i++) {
      rowIds[i] = i;
    }

    Vector[] rows = client1.get(rowIds, index);
    for (int i = 0; i < rowNum; i++) {
      for (int id : index) {
        Assert.assertEquals(((CompIntDoubleVector) rows[i]).get(id), deltaVec.get(id), zero);
      }
    }
  }

  public void testSparseDoubleCompUDF() throws Exception {

    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient client1 = worker.getPSAgent().getMatrixClient(SPARSE_DOUBLE_MAT_COMP, 0);
    int matrixW1Id = client1.getMatrixId();

    int[] index = genIndexs(feaNum, nnz);

    CompIntDoubleVector deltaVec = null;
    for (int rowId = 0; rowId < rowNum; rowId++) {
      long blockColNum =
        worker.getPSAgent().getMatrixMetaManager().getMatrixMeta(matrixW1Id).getBlockColNum();
      int partNum = (feaNum + (int) blockColNum - 1) / (int) blockColNum;
      IntDoubleVector[] subVecs = new IntDoubleVector[partNum];
      for (int i = 0; i < partNum; i++) {
        subVecs[i] = VFactory.sparseDoubleVector((int) blockColNum);
      }

      deltaVec = new CompIntDoubleVector(feaNum, subVecs, (int) blockColNum);
      for (int i = 0; i < nnz; i++)
        deltaVec.set(index[i], index[i]);

      client1.update(rowId, deltaVec);
    }

    int[] rowIds = new int[rowNum];
    for (int i = 0; i < rowNum; i++) {
      rowIds[i] = i;
    }

    Vector[] rows = client1.get(rowIds, index);
    for (int i = 0; i < rowNum; i++) {
      for (int id : index) {
        Assert.assertEquals(((CompIntDoubleVector) rows[i]).get(id), deltaVec.get(id), zero);
      }
    }
  }

  public void testDenseFloatUDF() throws Exception {
    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient client1 = worker.getPSAgent().getMatrixClient(DENSE_FLOAT_MAT, 0);

    int[] index = genIndexs(feaNum, nnz);
    IntFloatVector deltaVec = null;
    for (int rowId = 0; rowId < rowNum; rowId++) {
      deltaVec = new IntFloatVector(feaNum, new IntFloatDenseVectorStorage(feaNum));
      for (int i = 0; i < feaNum; i++)
        deltaVec.set(i, i);
      client1.update(rowId, deltaVec);
    }

    int[] rowIds = new int[rowNum];
    for (int i = 0; i < rowNum; i++) {
      rowIds[i] = i;
    }

    Vector[] rows = client1.get(rowIds, index);
    for (int i = 0; i < rowNum; i++) {
      for (int id : index) {
        Assert.assertEquals(((IntFloatVector) rows[i]).get(id), deltaVec.get(id), zero);
      }
    }

    client1.zero();
    IntFloatVector[] deltaVecs = new IntFloatVector[rowNum];
    for (int rowId = 0; rowId < rowNum; rowId++) {
      deltaVec = VFactory.sparseFloatVector(feaNum, index.length);
      for (int i = 0; i < index.length; i++) {
        deltaVec.set(index[i], index[i]);
      }
      deltaVecs[rowId] = deltaVec;
    }

    client1.update(rowIds, deltaVecs);

    rows = client1.get(rowIds, index);
    for (int i = 0; i < rowNum; i++) {
      for (int id : index) {
        Assert.assertEquals(((IntFloatVector) rows[i]).get(id), deltaVecs[i].get(id), zero);
      }
    }

    client1.zero();

    for (int rowId = 0; rowId < rowNum; rowId++) {
      deltaVec = VFactory.sparseFloatVector(feaNum, index.length);
      for (int i = 0; i < index.length; i++) {
        deltaVec.set(index[i], index[i]);
      }
      deltaVecs[rowId] = deltaVec;
    }

    client1.increment(rowIds, deltaVecs, true);

    rows = client1.get(rowIds, index);
    for (int i = 0; i < rowNum; i++) {
      for (int id : index) {
        Assert.assertEquals(((IntFloatVector) rows[i]).get(id), deltaVecs[i].get(id), zero);
      }
    }
  }

  public void testSparseFloatUDF() throws Exception {
    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient client1 = worker.getPSAgent().getMatrixClient(SPARSE_FLOAT_MAT, 0);

    int[] index = genIndexs(feaNum, nnz);
    IntFloatVector deltaVec = null;
    for (int rowId = 0; rowId < rowNum; rowId++) {
      deltaVec = new IntFloatVector(feaNum, new IntFloatSparseVectorStorage(feaNum));
      for (int i = 0; i < index.length; i++) {
        deltaVec.set(index[i], index[i]);
      }
      client1.update(rowId, deltaVec);
    }

    int[] rowIds = new int[rowNum];
    for (int i = 0; i < rowNum; i++) {
      rowIds[i] = i;
    }

    Vector[] rows = client1.get(rowIds, index);
    for (int i = 0; i < rowNum; i++) {
      for (int id : index) {
        Assert.assertEquals(((IntFloatVector) rows[i]).get(id), deltaVec.get(id), zero);
      }
      Assert.assertTrue(index.length == ((IntFloatVector) rows[i]).size());
    }

    client1.zero();
    IntFloatVector[] deltaVecs = new IntFloatVector[rowNum];
    for (int rowId = 0; rowId < rowNum; rowId++) {
      deltaVec = VFactory.sparseFloatVector(feaNum, index.length);
      for (int i = 0; i < index.length; i++) {
        deltaVec.set(index[i], index[i]);
      }
      deltaVecs[rowId] = deltaVec;
    }

    client1.update(rowIds, deltaVecs);

    rows = client1.get(rowIds, index);
    for (int i = 0; i < rowNum; i++) {
      for (int id : index) {
        Assert.assertEquals(((IntFloatVector) rows[i]).get(id), deltaVecs[i].get(id), zero);
      }
    }

    client1.zero();

    for (int rowId = 0; rowId < rowNum; rowId++) {
      deltaVec = VFactory.sparseFloatVector(feaNum, index.length);
      for (int i = 0; i < index.length; i++) {
        deltaVec.set(index[i], index[i]);
      }
      deltaVecs[rowId] = deltaVec;
    }

    client1.increment(rowIds, deltaVecs, true);

    rows = client1.get(rowIds, index);
    for (int i = 0; i < rowNum; i++) {
      for (int id : index) {
        Assert.assertEquals(((IntFloatVector) rows[i]).get(id), deltaVecs[i].get(id), zero);
      }
    }
  }

  public void testDenseFloatCompUDF() throws Exception {
    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient client1 = worker.getPSAgent().getMatrixClient(DENSE_FLOAT_MAT_COMP, 0);
    int matrixW1Id = client1.getMatrixId();

    int[] index = genIndexs(feaNum, nnz);

    CompIntFloatVector deltaVec = null;
    for (int rowId = 0; rowId < rowNum; rowId++) {
      long blockColNum =
        worker.getPSAgent().getMatrixMetaManager().getMatrixMeta(matrixW1Id).getBlockColNum();
      int partNum = (feaNum + (int) blockColNum - 1) / (int) blockColNum;
      IntFloatVector[] subVecs = new IntFloatVector[partNum];
      for (int i = 0; i < partNum; i++) {
        subVecs[i] = VFactory.denseFloatVector((int) blockColNum);
      }

      deltaVec = new CompIntFloatVector(feaNum, subVecs, (int) blockColNum);
      for (int i = 0; i < feaNum; i++)
        deltaVec.set(i, i);

      client1.update(rowId, deltaVec);
    }

    int[] rowIds = new int[rowNum];
    for (int i = 0; i < rowNum; i++) {
      rowIds[i] = i;
    }

    Vector[] rows = client1.get(rowIds, index);
    for (int i = 0; i < rowNum; i++) {
      for (int id : index) {
        Assert.assertEquals(((CompIntFloatVector) rows[i]).get(id), deltaVec.get(id), zero);
      }
    }
  }

  public void testSparseFloatCompUDF() throws Exception {

    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient client1 = worker.getPSAgent().getMatrixClient(SPARSE_FLOAT_MAT_COMP, 0);
    int matrixW1Id = client1.getMatrixId();

    int[] index = genIndexs(feaNum, nnz);

    CompIntFloatVector deltaVec = null;
    for (int rowId = 0; rowId < rowNum; rowId++) {
      long blockColNum =
        worker.getPSAgent().getMatrixMetaManager().getMatrixMeta(matrixW1Id).getBlockColNum();
      int partNum = (feaNum + (int) blockColNum - 1) / (int) blockColNum;
      IntFloatVector[] subVecs = new IntFloatVector[partNum];
      for (int i = 0; i < partNum; i++) {
        subVecs[i] = VFactory.denseFloatVector((int) blockColNum);
      }

      deltaVec = new CompIntFloatVector(feaNum, subVecs, (int) blockColNum);
      for (int i = 0; i < index.length; i++) {
        deltaVec.set(index[i], index[i]);
      }

      client1.update(rowId, deltaVec);
    }

    int[] rowIds = new int[rowNum];
    for (int i = 0; i < rowNum; i++) {
      rowIds[i] = i;
    }

    Vector[] rows = client1.get(rowIds, index);
    for (int i = 0; i < rowNum; i++) {
      for (int id : index) {
        Assert.assertEquals(((CompIntFloatVector) rows[i]).get(id), deltaVec.get(id), zero);
      }
      Assert.assertTrue(index.length == ((CompIntFloatVector) rows[i]).size());
    }

  }


  public void testDenseIntUDF() throws Exception {
    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient client1 = worker.getPSAgent().getMatrixClient(DENSE_INT_MAT, 0);
    int matrixW1Id = client1.getMatrixId();

    int[] index = genIndexs(feaNum, nnz);

    IntIntVector deltaVec = null;
    for (int rowId = 0; rowId < rowNum; rowId++) {
      deltaVec = new IntIntVector(feaNum, new IntIntDenseVectorStorage(feaNum));
      for (int i = 0; i < feaNum; i++)
        deltaVec.set(i, i);
      client1.update(rowId, deltaVec);
    }

    int[] rowIds = new int[rowNum];
    for (int i = 0; i < rowNum; i++) {
      rowIds[i] = i;
    }

    Vector[] rows = client1.get(rowIds, index);
    for (int i = 0; i < rowNum; i++) {
      for (int id : index) {
        Assert.assertEquals(((IntIntVector) rows[i]).get(id), deltaVec.get(id), zero);
      }
    }

    client1.zero();
    IntIntVector[] deltaVecs = new IntIntVector[rowNum];
    for (int rowId = 0; rowId < rowNum; rowId++) {
      deltaVec = VFactory.sparseIntVector(feaNum, index.length);
      for (int i = 0; i < index.length; i++) {
        deltaVec.set(index[i], index[i]);
      }
      deltaVecs[rowId] = deltaVec;
    }

    client1.update(rowIds, deltaVecs);

    rows = client1.get(rowIds, index);
    for (int i = 0; i < rowNum; i++) {
      for (int id : index) {
        Assert.assertEquals(((IntIntVector) rows[i]).get(id), deltaVecs[i].get(id), zero);
      }
    }

    client1.zero();

    for (int rowId = 0; rowId < rowNum; rowId++) {
      deltaVec = VFactory.sparseIntVector(feaNum, index.length);
      for (int i = 0; i < index.length; i++) {
        deltaVec.set(index[i], index[i]);
      }
      deltaVecs[rowId] = deltaVec;
    }

    client1.increment(rowIds, deltaVecs, true);

    rows = client1.get(rowIds, index);
    for (int i = 0; i < rowNum; i++) {
      for (int id : index) {
        Assert.assertEquals(((IntIntVector) rows[i]).get(id), deltaVecs[i].get(id), zero);
      }
    }
  }

  public void testSparseIntUDF() throws Exception {

    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient client1 = worker.getPSAgent().getMatrixClient(SPARSE_INT_MAT, 0);
    int matrixW1Id = client1.getMatrixId();

    int[] index = genIndexs(feaNum, nnz);

    IntIntVector deltaVec = null;
    for (int rowId = 0; rowId < rowNum; rowId++) {
      deltaVec = new IntIntVector(feaNum, new IntIntSparseVectorStorage(feaNum));
      for (int i = 0; i < index.length; i++) {
        deltaVec.set(index[i], index[i]);
      }
      client1.update(rowId, deltaVec);
    }

    int[] rowIds = new int[rowNum];
    for (int i = 0; i < rowNum; i++) {
      rowIds[i] = i;
    }

    Vector[] rows = client1.get(rowIds, index);
    for (int i = 0; i < rowNum; i++) {
      for (int id : index) {
        Assert.assertEquals(((IntIntVector) rows[i]).get(id), deltaVec.get(id), zero);
      }
      Assert.assertTrue(index.length == ((IntIntVector) rows[i]).size());
    }

    client1.zero();
    IntIntVector[] deltaVecs = new IntIntVector[rowNum];
    for (int rowId = 0; rowId < rowNum; rowId++) {
      deltaVec = VFactory.sparseIntVector(feaNum, index.length);
      for (int i = 0; i < index.length; i++) {
        deltaVec.set(index[i], index[i]);
      }
      deltaVecs[rowId] = deltaVec;
    }

    client1.update(rowIds, deltaVecs);

    rows = client1.get(rowIds, index);
    for (int i = 0; i < rowNum; i++) {
      for (int id : index) {
        Assert.assertEquals(((IntIntVector) rows[i]).get(id), deltaVecs[i].get(id), zero);
      }
    }

    client1.zero();

    for (int rowId = 0; rowId < rowNum; rowId++) {
      deltaVec = VFactory.sparseIntVector(feaNum, index.length);
      for (int i = 0; i < index.length; i++) {
        deltaVec.set(index[i], index[i]);
      }
      deltaVecs[rowId] = deltaVec;
    }

    client1.increment(rowIds, deltaVecs, true);

    rows = client1.get(rowIds, index);
    for (int i = 0; i < rowNum; i++) {
      for (int id : index) {
        Assert.assertEquals(((IntIntVector) rows[i]).get(id), deltaVecs[i].get(id), zero);
      }
    }
  }

  public void testDenseIntCompUDF() throws Exception {
    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient client1 = worker.getPSAgent().getMatrixClient(DENSE_INT_MAT_COMP, 0);
    int matrixW1Id = client1.getMatrixId();

    int[] index = genIndexs(feaNum, nnz);

    CompIntIntVector deltaVec = null;
    for (int rowId = 0; rowId < rowNum; rowId++) {
      long blockColNum =
        worker.getPSAgent().getMatrixMetaManager().getMatrixMeta(matrixW1Id).getBlockColNum();
      int partNum = (feaNum + (int) blockColNum - 1) / (int) blockColNum;
      IntIntVector[] subVecs = new IntIntVector[partNum];
      for (int i = 0; i < partNum; i++) {
        subVecs[i] = VFactory.denseIntVector((int) blockColNum);
      }

      deltaVec = new CompIntIntVector(feaNum, subVecs, (int) blockColNum);
      for (int i = 0; i < feaNum; i++)
        deltaVec.set(i, i);

      client1.update(rowId, deltaVec);
    }

    int[] rowIds = new int[rowNum];
    for (int i = 0; i < rowNum; i++) {
      rowIds[i] = i;
    }

    Vector[] rows = client1.get(rowIds, index);
    for (int i = 0; i < rowNum; i++) {
      for (int id : index) {
        Assert.assertEquals(((CompIntIntVector) rows[i]).get(id), deltaVec.get(id), zero);
      }
    }
  }

  public void testDenseLongCompUDF() throws Exception {
    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient client1 = worker.getPSAgent().getMatrixClient(DENSE_LONG_MAT_COMP, 0);
    int matrixW1Id = client1.getMatrixId();

    int[] index = genIndexs(feaNum, nnz);

    CompIntLongVector deltaVec = null;
    for (int rowId = 0; rowId < rowNum; rowId++) {
      long blockColNum =
        worker.getPSAgent().getMatrixMetaManager().getMatrixMeta(matrixW1Id).getBlockColNum();
      int partNum = (feaNum + (int) blockColNum - 1) / (int) blockColNum;
      IntLongVector[] subVecs = new IntLongVector[partNum];
      for (int i = 0; i < partNum; i++) {
        subVecs[i] = VFactory.denseLongVector((int) blockColNum);
      }

      deltaVec = new CompIntLongVector(feaNum, subVecs, (int) blockColNum);
      for (int i = 0; i < feaNum; i++)
        deltaVec.set(i, i);

      client1.update(rowId, deltaVec);
    }

    int[] rowIds = new int[rowNum];
    for (int i = 0; i < rowNum; i++) {
      rowIds[i] = i;
    }

    Vector[] rows = client1.get(rowIds, index);
    for (int i = 0; i < rowNum; i++) {
      for (int id : index) {
        Assert.assertEquals(((CompIntLongVector) rows[i]).get(id), deltaVec.get(id), zero);
      }
    }
  }

  public void testSparseLongCompUDF() throws Exception {
    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient client1 = worker.getPSAgent().getMatrixClient(SPARSE_LONG_MAT_COMP, 0);
    int matrixW1Id = client1.getMatrixId();

    int[] index = genIndexs(feaNum, nnz);

    CompIntLongVector deltaVec = null;
    for (int rowId = 0; rowId < rowNum; rowId++) {
      long blockColNum =
        worker.getPSAgent().getMatrixMetaManager().getMatrixMeta(matrixW1Id).getBlockColNum();
      int partNum = (feaNum + (int) blockColNum - 1) / (int) blockColNum;
      IntLongVector[] subVecs = new IntLongVector[partNum];
      for (int i = 0; i < partNum; i++) {
        subVecs[i] = VFactory.sparseLongVector((int) blockColNum);
      }

      deltaVec = new CompIntLongVector(feaNum, subVecs, (int) blockColNum);
      for (int i = 0; i < index.length; i++) {
        deltaVec.set(index[i], index[i]);
      }

      client1.update(rowId, deltaVec);
    }

    int[] rowIds = new int[rowNum];
    for (int i = 0; i < rowNum; i++) {
      rowIds[i] = i;
    }

    Vector[] rows = client1.get(rowIds, index);
    for (int i = 0; i < rowNum; i++) {
      for (int id : index) {
        Assert.assertEquals(((CompIntLongVector) rows[i]).get(id), deltaVec.get(id), zero);
      }
      Assert.assertTrue(index.length == ((CompIntLongVector) rows[i]).size());
    }
  }

  public void testDenseLongUDF() throws Exception {
    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient client1 = worker.getPSAgent().getMatrixClient(DENSE_LONG_MAT, 0);
    int matrixW1Id = client1.getMatrixId();

    int[] index = genIndexs(feaNum, nnz);

    IntLongVector deltaVec = null;
    for (int rowId = 0; rowId < rowNum; rowId++) {
      deltaVec = new IntLongVector(feaNum, new IntLongDenseVectorStorage(feaNum));
      for (int i = 0; i < feaNum; i++)
        deltaVec.set(i, i);
      client1.update(rowId, deltaVec);
    }

    int[] rowIds = new int[rowNum];
    for (int i = 0; i < rowNum; i++) {
      rowIds[i] = i;
    }

    Vector[] rows = client1.get(rowIds, index);
    for (int i = 0; i < rowNum; i++) {
      for (int id : index) {
        Assert.assertEquals(((IntLongVector) rows[i]).get(id), deltaVec.get(id), zero);
      }
    }

    client1.zero();
    IntLongVector[] deltaVecs = new IntLongVector[rowNum];
    for (int rowId = 0; rowId < rowNum; rowId++) {
      deltaVec = VFactory.sparseLongVector(feaNum, index.length);
      for (int i = 0; i < index.length; i++) {
        deltaVec.set(index[i], index[i]);
      }
      deltaVecs[rowId] = deltaVec;
    }

    client1.update(rowIds, deltaVecs);

    rows = client1.get(rowIds, index);
    for (int i = 0; i < rowNum; i++) {
      for (int id : index) {
        Assert.assertEquals(((IntLongVector) rows[i]).get(id), deltaVecs[i].get(id), zero);
      }
    }

    client1.zero();

    for (int rowId = 0; rowId < rowNum; rowId++) {
      deltaVec = VFactory.sparseLongVector(feaNum, index.length);
      for (int i = 0; i < index.length; i++) {
        deltaVec.set(index[i], index[i]);
      }
      deltaVecs[rowId] = deltaVec;
    }

    client1.increment(rowIds, deltaVecs, true);

    rows = client1.get(rowIds, index);
    for (int i = 0; i < rowNum; i++) {
      for (int id : index) {
        Assert.assertEquals(((IntLongVector) rows[i]).get(id), deltaVecs[i].get(id), zero);
      }
    }
  }

  public void testSparseLongUDF() throws Exception {

    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient client1 = worker.getPSAgent().getMatrixClient(SPARSE_LONG_MAT, 0);
    int matrixW1Id = client1.getMatrixId();

    int[] index = genIndexs(feaNum, nnz);
    IntLongVector deltaVec = null;
    for (int rowId = 0; rowId < rowNum; rowId++) {
      deltaVec = new IntLongVector(feaNum, new IntLongSparseVectorStorage(feaNum));
      for (int i = 0; i < feaNum; i++)
        deltaVec.set(i, i);
      client1.update(rowId, deltaVec);
    }

    int[] rowIds = new int[rowNum];
    for (int i = 0; i < rowNum; i++) {
      rowIds[i] = i;
    }

    Vector[] rows = client1.get(rowIds, index);
    for (int i = 0; i < rowNum; i++) {
      for (int id : index) {
        Assert.assertEquals(((IntLongVector) rows[i]).get(id), deltaVec.get(id), zero);
      }
    }

    client1.zero();
    IntLongVector[] deltaVecs = new IntLongVector[rowNum];
    for (int rowId = 0; rowId < rowNum; rowId++) {
      deltaVec = VFactory.sparseLongVector(feaNum, index.length);
      for (int i = 0; i < index.length; i++) {
        deltaVec.set(index[i], index[i]);
      }
      deltaVecs[rowId] = deltaVec;
    }

    client1.update(rowIds, deltaVecs);

    rows = client1.get(rowIds, index);
    for (int i = 0; i < rowNum; i++) {
      for (int id : index) {
        Assert.assertEquals(((IntLongVector) rows[i]).get(id), deltaVecs[i].get(id), zero);
      }
    }

    client1.zero();

    for (int rowId = 0; rowId < rowNum; rowId++) {
      deltaVec = VFactory.sparseLongVector(feaNum, index.length);
      for (int i = 0; i < index.length; i++) {
        deltaVec.set(index[i], index[i]);
      }
      deltaVecs[rowId] = deltaVec;
    }

    client1.increment(rowIds, deltaVecs, true);

    rows = client1.get(rowIds, index);
    for (int i = 0; i < rowNum; i++) {
      for (int id : index) {
        Assert.assertEquals(((IntLongVector) rows[i]).get(id), deltaVecs[i].get(id), zero);
      }
    }
  }

  public void testSparseIntCompUDF() throws Exception {

    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient client1 = worker.getPSAgent().getMatrixClient(SPARSE_INT_MAT_COMP, 0);
    int matrixW1Id = client1.getMatrixId();
    MatrixMeta meta = worker.getPSAgent().getMatrix(SPARSE_INT_MAT_COMP);

    int[] index = genIndexs(feaNum, nnz);


    CompIntIntVector deltaVec = null;
    for (int rowId = 0; rowId < rowNum; rowId++) {
      long blockColNum =
        worker.getPSAgent().getMatrixMetaManager().getMatrixMeta(matrixW1Id).getBlockColNum();
      int partNum = (feaNum + (int) blockColNum - 1) / (int) blockColNum;
      IntIntVector[] subVecs = new IntIntVector[partNum];
      for (int i = 0; i < partNum; i++) {
        subVecs[i] = VFactory.sparseIntVector((int) blockColNum);
      }

      deltaVec = new CompIntIntVector(feaNum, subVecs, (int) blockColNum);
      for (int i = 0; i < index.length; i++) {
        deltaVec.set(index[i], index[i]);
      }

      client1.update(rowId, deltaVec);
    }

    int[] rowIds = new int[rowNum];
    for (int i = 0; i < rowNum; i++) {
      rowIds[i] = i;
    }

    Vector[] rows = client1.get(rowIds, index);
    for (int i = 0; i < rowNum; i++) {
      for (int id : index) {
        Assert.assertEquals(((CompIntIntVector) rows[i]).get(id), deltaVec.get(id), zero);
      }
      Assert.assertTrue(index.length == ((CompIntIntVector) rows[i]).size());
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

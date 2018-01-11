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

package com.tencent.angel.ml.matrix.psf.get.enhance.indexed;

import com.google.protobuf.ServiceException;
import com.tencent.angel.client.AngelClient;
import com.tencent.angel.client.AngelClientFactory;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.exception.InvalidParameterException;
import com.tencent.angel.localcluster.LocalClusterContext;
import com.tencent.angel.ml.math.vector.DenseDoubleVector;
import com.tencent.angel.ml.math.vector.SparseDoubleVector;
import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ml.matrix.psf.get.single.GetRowResult;
import com.tencent.angel.protobuf.generated.MLProtos;
import com.tencent.angel.ps.PSAttemptId;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.psagent.DummyTask;
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

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ExecutionException;

public class IndexGetFuncTest {
  public static String DENSE_DOUBLE_MAT = "dense_double_mat";
  public static String SPARSE_DOUBLE_MAT = "sparse_double_mat";
  public static String SPARSE_DOUBLE_LONG_MAT = "sparse_double_long_mat";

  private static final Log LOG = LogFactory.getLog(IndexGetFuncTest.class);
  private static final String LOCAL_FS = LocalFileSystem.DEFAULT_FS;
  private static final String TMP_PATH = System.getProperty("java.io.tmpdir", "/tmp");
  private AngelClient angelClient;
  private ParameterServerId psId;
  private PSAttemptId psAttempt0Id;
  private WorkerId workerId;
  private WorkerAttemptId workerAttempt0Id;

  int feaNum = 1000000;
  int nnz = 1000;

  static {
    PropertyConfigurator.configure("../conf/log4j.properties");
  }

  @Before
  public void setup() throws Exception {
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

    // add dense double matrix
    MatrixContext dMat = new MatrixContext();
    dMat.setName(DENSE_DOUBLE_MAT);
    dMat.setRowNum(1);
    dMat.setColNum(feaNum);
    dMat.setMaxColNumInBlock(feaNum / 3);
    dMat.setRowType(RowType.T_DOUBLE_DENSE);
    angelClient.addMatrix(dMat);

    // add sparse double matrix
    MatrixContext sMat = new MatrixContext();
    sMat.setName(SPARSE_DOUBLE_MAT);
    sMat.setRowNum(1);
    sMat.setColNum(feaNum);
    sMat.setMaxColNumInBlock(feaNum / 3);
    sMat.setRowType(RowType.T_DOUBLE_DENSE);
    angelClient.addMatrix(sMat);

    // add sparse double long key matrix
    MatrixContext lMat = new MatrixContext();
    lMat.setName(SPARSE_DOUBLE_LONG_MAT);
    lMat.setRowNum(1);
    lMat.setColNum(feaNum);
    lMat.setMaxColNumInBlock(feaNum / 3);
    lMat.setRowType(RowType.T_DOUBLE_DENSE);
    angelClient.addMatrix(lMat);

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

  @Test
  public void test() throws ServiceException, ExecutionException, InvalidParameterException,
      InterruptedException, IOException {
    testDenseDoubleUDF();
  }

  public void testDenseDoubleUDF() throws ServiceException, IOException, InvalidParameterException,
      AngelException, InterruptedException, ExecutionException {
    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient client1 = worker.getPSAgent().getMatrixClient(DENSE_DOUBLE_MAT, 0);
    int matrixW1Id = client1.getMatrixId();

    int[] index = genIndexs(feaNum, nnz);

    DenseDoubleVector deltaVec = new DenseDoubleVector(feaNum);
    for (int i = 0; i < feaNum; i++)
      deltaVec.set(i, i);
    deltaVec.setRowId(0);

    client1.increment(deltaVec);
    client1.clock().get();

    IndexGetFunc func = new IndexGetFunc(new IndexGetParam(matrixW1Id, 0, index));
    SparseDoubleVector row = (SparseDoubleVector) ((GetRowResult) client1.get(func)).getRow();
    for (int id: index) {
      Assert.assertTrue(row.get(id) == deltaVec.get(id));
    }
    Assert.assertTrue(index.length == row.size());

  }

  @Test
  public void testSparseDoubleUDF() throws ServiceException, IOException, InvalidParameterException,
      AngelException, InterruptedException, ExecutionException {

    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient client1 = worker.getPSAgent().getMatrixClient(SPARSE_DOUBLE_MAT, 0);
    int matrixW1Id = client1.getMatrixId();

    int[] index = genIndexs(feaNum, nnz);

    SparseDoubleVector deltaVec = new SparseDoubleVector(feaNum);
    for (int i = 0; i < feaNum; i++)
      deltaVec.set(i, i);
    deltaVec.setRowId(0);

    client1.increment(deltaVec);
    client1.clock().get();

    IndexGetFunc func = new IndexGetFunc(new IndexGetParam(matrixW1Id, 0, index));

    SparseDoubleVector row = (SparseDoubleVector) ((GetRowResult) client1.get(func)).getRow();
    for (int id: index) {
      System.out.println("id=" + id + ", value=" + row.get(id));
      Assert.assertTrue(row.get(id) == deltaVec.get(id));
    }

    Assert.assertTrue(index.length == row.size());
  }

  public static int[] genIndexs(int feaNum, int nnz) {

    int[] sortedIndex = new int[nnz];
    Random random = new Random(System.currentTimeMillis());
    sortedIndex[0] = random.nextInt(feaNum/nnz);
    for (int i = 1; i < nnz; i ++) {
      int rand = random.nextInt( (feaNum - sortedIndex[i-1]) / (nnz - i) );
      if (rand==0) rand = 1;
      sortedIndex[i] = rand + sortedIndex[i-1];
    }

    return sortedIndex;
  }



  @After
  public void stop() throws AngelException {
    LOG.info("stop local cluster");
    angelClient.stop();
  }
}

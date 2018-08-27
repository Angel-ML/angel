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


package com.tencent.angel.master;

import com.tencent.angel.client.AngelClient;
import com.tencent.angel.client.AngelClientFactory;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.localcluster.LocalClusterContext;
import com.tencent.angel.ml.math2.storage.IntDoubleDenseVectorStorage;
import com.tencent.angel.ml.math2.storage.IntDoubleSparseVectorStorage;
import com.tencent.angel.ml.math2.vector.IntDoubleVector;
import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ml.matrix.psf.get.enhance.indexed.IndexGetTest;
import com.tencent.angel.ml.matrix.psf.get.getrow.GetRow;
import com.tencent.angel.ml.matrix.psf.get.getrow.GetRowParam;
import com.tencent.angel.ml.matrix.psf.get.getrow.GetRowResult;
import com.tencent.angel.model.MatrixLoadContext;
import com.tencent.angel.model.MatrixSaveContext;
import com.tencent.angel.model.ModelLoadContext;
import com.tencent.angel.model.ModelSaveContext;
import com.tencent.angel.ps.PSAttemptId;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.psagent.matrix.MatrixClient;
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

import java.util.ArrayList;
import java.util.List;

public class SaveTest {
  public static String DENSE_DOUBLE_MAT = "dense_double_mat";
  public static String SPARSE_DOUBLE_MAT = "sparse_double_mat";
  private static final Log LOG = LogFactory.getLog(IndexGetTest.class);
  private static final String LOCAL_FS = LocalFileSystem.DEFAULT_FS;
  private static final String TMP_PATH = System.getProperty("java.io.tmpdir", "/tmp");
  private AngelClient angelClient;
  private ParameterServerId psId;
  private PSAttemptId psAttempt0Id;
  private WorkerId workerId;
  private WorkerAttemptId workerAttempt0Id;

  int feaNum = 1000000;
  int nnz = 1000;
  int epochNum = 5;
  double zero = 0.00000000001;

  static {
    PropertyConfigurator.configure("../conf/log4j.properties");
  }

  @Before public void setup() throws Exception {
    // set basic configuration keys
    Configuration conf = new Configuration();
    conf.setBoolean("mapred.mapper.new-api", true);
    conf.setBoolean(AngelConf.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST, true);
    conf.set(AngelConf.ANGEL_TASK_USER_TASKCLASS,
      com.tencent.angel.psagent.DummyTask.class.getName());

    // use local deploy mode and dummy dataspliter
    conf.set(AngelConf.ANGEL_DEPLOY_MODE, "LOCAL");
    conf.setBoolean(AngelConf.ANGEL_AM_USE_DUMMY_DATASPLITER, true);
    conf.set(AngelConf.ANGEL_INPUTFORMAT_CLASS, CombineTextInputFormat.class.getName());
    conf.set(AngelConf.ANGEL_SAVE_MODEL_PATH, LOCAL_FS + TMP_PATH + "/out1");
    conf.set(AngelConf.ANGEL_TRAIN_DATA_PATH, LOCAL_FS + TMP_PATH + "/in");
    conf.set(AngelConf.ANGEL_LOG_PATH, LOCAL_FS + TMP_PATH + "/log");

    conf.setInt(AngelConf.ANGEL_WORKERGROUP_NUMBER, 1);
    conf.setInt(AngelConf.ANGEL_PS_NUMBER, 1);
    conf.setInt(AngelConf.ANGEL_WORKER_TASK_NUMBER, 1);
    conf.setBoolean(AngelConf.ANGEL_SAVE_MODEL_EPOCH_TIGGER_ENABLE, true);
    conf.setInt(AngelConf.ANGEL_SAVE_MODEL_EVERY_HOWMANY_EPOCHS, 1);

    // get a angel client
    angelClient = AngelClientFactory.get(conf);

    // add dense double matrix
    MatrixContext dMat = new MatrixContext();
    dMat.setName(DENSE_DOUBLE_MAT);
    dMat.setRowNum(2);
    dMat.setColNum(feaNum);
    dMat.setMaxRowNumInBlock(1);
    dMat.setMaxColNumInBlock(feaNum / 2);
    dMat.setRowType(RowType.T_DOUBLE_DENSE);
    angelClient.addMatrix(dMat);

    // add sparse double matrix
    MatrixContext sMat = new MatrixContext();
    sMat.setName(SPARSE_DOUBLE_MAT);
    sMat.setRowNum(2);
    sMat.setColNum(feaNum);
    sMat.setMaxRowNumInBlock(1);
    sMat.setMaxColNumInBlock(feaNum / 2);
    sMat.setRowType(RowType.T_DOUBLE_SPARSE);
    angelClient.addMatrix(sMat);

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

  /*@Test
  public void  testMap() {
    Int2DoubleOpenHashMap map = new Int2DoubleOpenHashMap(10000000);
    Random r = new Random();
    for(int i = 0; i < 10000000; i++) {
      map.put(r.nextInt(), r.nextDouble());
    }
    testMap2(map, 100);
    testMap1(map, 100);
  }

  private void testMap1(Int2DoubleOpenHashMap map, int tryTime) {
    long ts = System.currentTimeMillis();
    for(int i = 0; i < tryTime; i++) {
      int [] indices = map.keySet().toIntArray();
      double [] values = map.values().toDoubleArray();
    }
    LOG.info("testMap1 use time=" + (System.currentTimeMillis() - ts));
  }

  private void testMap2(Int2DoubleOpenHashMap map, int tryTime) {
    long ts = System.currentTimeMillis();
    for(int i = 0; i < tryTime; i++) {
      int [] indices = new int[map.size()];
      double [] values = new double[map.size()];
      ObjectIterator<Int2DoubleMap.Entry>
        iter = map.int2DoubleEntrySet().fastIterator();
      int index = 0;
      Int2DoubleMap.Entry entry;
      while(iter.hasNext()) {
        entry = iter.next();
        indices[index] = entry.getIntKey();
        values[index] = entry.getDoubleValue();
        index++;
      }
    }
    LOG.info("testMap2 use time=" + (System.currentTimeMillis() - ts));
  }
  */


  @Test public void testSave() throws Exception {
    MatrixClient client =
      LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker().getPSAgent()
        .getMatrixClient(DENSE_DOUBLE_MAT, 0);

    MatrixClient clientSparse =
      LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker().getPSAgent()
        .getMatrixClient(SPARSE_DOUBLE_MAT, 0);

    int epochIndex = 0;
    String outPath = LOCAL_FS + TMP_PATH + "/out/1";
    double sum = 0.0;

    while (epochIndex < epochNum) {
      GetRow func = new GetRow(new GetRowParam(client.getMatrixId(), 0));
      IntDoubleVector vector = (IntDoubleVector) ((GetRowResult) client.get(func)).getRow();
      LOG.info("sum of dense vector = " + sum(vector));
      double[] delta = new double[feaNum];
      for (int i = 0; i < feaNum; i++) {
        delta[i] = 1.0;
      }
      sum += feaNum;
      IntDoubleVector deltaVec =
        new IntDoubleVector(feaNum, new IntDoubleDenseVectorStorage(delta));
      client.increment(0, deltaVec, true);

      IntDoubleVector sparseVec = (IntDoubleVector) clientSparse.getRow(0);
      LOG.info("sum of sparse vector = " + sum(sparseVec));
      int[] sparseIndexes = new int[feaNum];
      double[] sparseValues = new double[feaNum];
      for (int i = 0; i < feaNum; i++) {
        sparseIndexes[i] = i;
        sparseValues[i] = 1.0;
      }
      IntDoubleVector sparseDeltaVec = new IntDoubleVector(feaNum,
        new IntDoubleSparseVectorStorage(feaNum, sparseIndexes, sparseValues));
      clientSparse.increment(0, sparseDeltaVec, true);

      outPath = LOCAL_FS + TMP_PATH + "/out/" + String.valueOf(epochIndex);
      ModelSaveContext saveContext = new ModelSaveContext(outPath);
      List<Integer> indexes = new ArrayList<>();
      indexes.add(0);
      indexes.add(1);
      saveContext.addMatrix(new MatrixSaveContext(DENSE_DOUBLE_MAT, indexes));
      saveContext.addMatrix(new MatrixSaveContext(SPARSE_DOUBLE_MAT, indexes));
      angelClient.save(saveContext);
      epochIndex++;
    }

    client.zero();
    clientSparse.zero();

    ModelLoadContext loadContext = new ModelLoadContext(outPath);
    loadContext.addMatrix(new MatrixLoadContext(DENSE_DOUBLE_MAT));
    loadContext.addMatrix(new MatrixLoadContext(SPARSE_DOUBLE_MAT));
    angelClient.load(loadContext);

    IntDoubleVector vector = (IntDoubleVector) client.getRow(0);
    LOG.info("after save, load sum of dense vector = " + sum(vector));
    Assert.assertEquals(sum(vector), sum, zero);

    IntDoubleVector sparseVec = (IntDoubleVector) clientSparse.getRow(0);
    LOG.info("after save, load sum of sparse vector = " + sum(sparseVec));
    Assert.assertEquals(sum(vector), sum, zero);

    epochIndex = 0;
    while (epochIndex < epochNum) {
      vector = (IntDoubleVector) client.getRow(0);
      LOG.info("sum of dense vector = " + sum(vector));
      double[] delta = new double[feaNum];
      for (int i = 0; i < feaNum; i++) {
        delta[i] = 1.0;
      }
      sum += feaNum;
      IntDoubleVector deltaVec =
        new IntDoubleVector(feaNum, new IntDoubleDenseVectorStorage(delta));
      client.increment(0, deltaVec, true);

      sparseVec = (IntDoubleVector) clientSparse.getRow(0);
      LOG.info("sum of sparse vector = " + sum(sparseVec));
      int[] sparseIndexes = new int[feaNum];
      double[] sparseValues = new double[feaNum];
      for (int i = 0; i < feaNum; i++) {
        sparseIndexes[i] = i;
        sparseValues[i] = 1.0;
      }
      IntDoubleVector sparseDeltaVec = new IntDoubleVector(feaNum,
        new IntDoubleSparseVectorStorage(feaNum, sparseIndexes, sparseValues));
      clientSparse.increment(0, sparseDeltaVec, true);
      epochIndex++;
    }

    loadContext = new ModelLoadContext(outPath);
    loadContext.addMatrix(new MatrixLoadContext(DENSE_DOUBLE_MAT));
    loadContext.addMatrix(new MatrixLoadContext(SPARSE_DOUBLE_MAT));
    angelClient.load(loadContext);

    vector = (IntDoubleVector) client.getRow(0);
    LOG.info("after load sum of dense vector = " + sum(vector));
    Assert.assertEquals(sum(vector), sum, zero);

    sparseVec = (IntDoubleVector) clientSparse.getRow(0);
    LOG.info("after load sum of sparse vector = " + sum(sparseVec));
    Assert.assertEquals(sum(vector), sum, zero);
  }

  private double sum(IntDoubleVector vector) {
    double[] values = vector.getStorage().getValues();
    double sum = 0.0;
    for (int i = 0; i < values.length; i++) {
      sum += values[i];
    }
    return sum;
  }

  @After public void stop() throws AngelException {
    try {
      LOG.info("stop local cluster");
      angelClient.stop();
    } catch (Exception x) {
      LOG.error("stop failed ", x);
      throw x;
    }
  }
}

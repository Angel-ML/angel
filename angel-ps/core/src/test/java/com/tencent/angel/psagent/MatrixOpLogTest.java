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

import com.google.protobuf.ServiceException;
import com.tencent.angel.client.AngelClient;
import com.tencent.angel.client.AngelClientFactory;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.conf.MatrixConf;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.exception.InvalidParameterException;
import com.tencent.angel.localcluster.LocalClusterContext;
import com.tencent.angel.ml.math2.VFactory;
import com.tencent.angel.ml.math2.vector.IntDoubleVector;
import com.tencent.angel.ml.math2.vector.IntIntVector;
import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.ml.matrix.RowType;
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
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

public class MatrixOpLogTest {
  private static final Log LOG = LogFactory.getLog(MatrixOpLogTest.class);
  private static final String LOCAL_FS = LocalFileSystem.DEFAULT_FS;
  private static final String TMP_PATH = System.getProperty("java.io.tmpdir", "/tmp");
  private AngelClient angelClient;
  private ParameterServerId psId;
  private PSAttemptId psAttempt0Id;
  private WorkerId workerId;
  private WorkerAttemptId workerAttempt0Id;

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

    // get a angel client
    angelClient = AngelClientFactory.get(conf);

    // add matrix
    MatrixContext mMatrix = new MatrixContext();
    mMatrix.setName("w1");
    mMatrix.setRowNum(100);
    mMatrix.setColNum(100000);
    mMatrix.setMaxRowNumInBlock(10);
    mMatrix.setMaxColNumInBlock(50000);
    mMatrix.setRowType(RowType.T_INT_DENSE);
    mMatrix.set(MatrixConf.MATRIX_OPLOG_ENABLEFILTER, "false");
    mMatrix.set(MatrixConf.MATRIX_HOGWILD, "true");
    mMatrix.set(MatrixConf.MATRIX_AVERAGE, "false");
    angelClient.addMatrix(mMatrix);

    // add matrix
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
    angelClient.addMatrix(mMatrix2);

    angelClient.startPSServer();
    angelClient.run();
    Thread.sleep(5000);
    psId = new ParameterServerId(0);
    psAttempt0Id = new PSAttemptId(psId, 0);
    WorkerGroupId workerGroupId = new WorkerGroupId(0);
    workerId = new WorkerId(workerGroupId, 0);
    workerAttempt0Id = new WorkerAttemptId(workerId, 0);
  }

  /*
  @Test
  public void testUDF() throws ServiceException, IOException, InvalidParameterException,
    AngelException, InterruptedException, ExecutionException {
    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient w1Task0Client = worker.getPSAgent().getMatrixClient("w1", 0);
    MatrixClient w1Task1Client = worker.getPSAgent().getMatrixClient("w1", 1);
    int matrixW1Id = w1Task0Client.getMatrixId();
    List<Integer> rowIndexes = new ArrayList<Integer>();
    for(int i = 0; i < 100; i++) {
      rowIndexes.add(i);
    }

    GetRowsFunc func = new GetRowsFunc(new GetRowsParam(matrixW1Id, rowIndexes));

    int [] delta = new int[100000];
    for(int i = 0; i < 100000; i++) {
      delta[i] = 1;
    }

    //DenseIntVector deltaVec = new DenseIntVector(100000, delta);
    //deltaVec.setMatrixId(matrixW1Id);
    //deltaVec.setRowId(0);

    int index = 0;
    while(index++ < 10) {
      Map<Integer, Vector> rows = ((GetRowsResult) w1Task0Client.get(func)).getRows();
      for(Entry<Integer, Vector> rowEntry:rows.entrySet()) {
        LOG.info("index " + rowEntry.getKey() + " sum of w1 = " + sum((IntIntVector) rowEntry.getValue()));
      }

      for(int i = 0; i < 100; i++) {
        IntIntVector deltaVec = new IntIntVector(100000, new IntIntDenseVectorStorage(delta));
        deltaVec.setMatrixId(matrixW1Id);
        deltaVec.setRowId(i);

        w1Task0Client.increment(deltaVec);

        deltaVec = new IntIntVector(100000, new IntIntDenseVectorStorage(delta));
        deltaVec.setMatrixId(matrixW1Id);
        deltaVec.setRowId(i);
        w1Task1Client.increment(deltaVec);
      }

      w1Task0Client.clock().get();
      w1Task1Client.clock().get();
    }
  }*/

  @Test public void testGetRow()
    throws ServiceException, IOException, InvalidParameterException, AngelException,
    InterruptedException, ExecutionException {
    Worker worker = LocalClusterContext.get().getWorker(workerAttempt0Id).getWorker();
    MatrixClient w2Task0Client = worker.getPSAgent().getMatrixClient("w2", 0);
    int matrixW2Id = w2Task0Client.getMatrixId();

    double[] delta = new double[100000];
    Random r = new Random();
    for (int i = 0; i < delta.length; i++) {
      delta[i] = r.nextDouble();
    }

    Vector deltaVec = VFactory.denseDoubleVector(delta);
    deltaVec.setRowId(0);
    deltaVec.setMatrixId(matrixW2Id);
    w2Task0Client.increment(deltaVec);
    w2Task0Client.clock().get();

    IntDoubleVector row = (IntDoubleVector) w2Task0Client.getRow(0);
    double[] values = row.getStorage().getValues();
    for (int i = 0; i < 100000; i++) {
      assertEquals(delta[i], values[i], 0.0000001);
    }
  }

  private int sum(IntIntVector vec) {
    int[] values = vec.getStorage().getValues();
    int sum = 0;
    for (int i = 0; i < values.length; i++) {
      sum += values[i];
    }
    return sum;
  }

  @After public void stop() throws AngelException {
    LOG.info("stop local cluster");
    angelClient.stop();
  }
}

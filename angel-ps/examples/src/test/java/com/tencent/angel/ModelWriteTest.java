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

package com.tencent.angel;

import com.tencent.angel.client.AngelClient;
import com.tencent.angel.client.AngelClientFactory;
import com.tencent.angel.conf.AngelConfiguration;
import com.tencent.angel.conf.MatrixConfiguration;
import com.tencent.angel.example.ModelWriteTask;
import com.tencent.angel.ml.feature.LabeledData;
import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.ml.model.AlgorithmModel;
import com.tencent.angel.ml.model.PSModel;
import com.tencent.angel.protobuf.generated.MLProtos;
import com.tencent.angel.worker.predict.PredictResult;
import com.tencent.angel.worker.storage.Storage;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.log4j.PropertyConfigurator;
import org.junit.After;
import org.junit.Test;

public class ModelWriteTest {
  private static final Log LOG = LogFactory.getLog(ModelWriteTest.class);
  private static final String LOCAL_FS = LocalFileSystem.DEFAULT_FS;
  private static final String TMP_PATH = System.getProperty("java.io.tmpdir", "/tmp");
  private AngelClient angelClient;

  static {
    PropertyConfigurator.configure("../log4j.properties");
  }



  public class WriteModel extends AlgorithmModel {

    @Override
    public Storage<PredictResult> predict(Storage<LabeledData> storage) {
      return null;
    }

    public WriteModel(int psNumber, String path) {
      MatrixContext doubleDenseMatrix = new MatrixContext();
      doubleDenseMatrix.setName("DoubleDense");
      doubleDenseMatrix.setRowNum(2);
      doubleDenseMatrix.setColNum(100);
      doubleDenseMatrix.setMaxRowNumInBlock(2);
      doubleDenseMatrix.setMaxColNumInBlock(100 / psNumber);
      doubleDenseMatrix.setRowType(MLProtos.RowType.T_DOUBLE_DENSE);
      doubleDenseMatrix.set(MatrixConfiguration.MATRIX_OPLOG_TYPE, "DENSE_DOUBLE");
      doubleDenseMatrix.set(MatrixConfiguration.MATRIX_AVERAGE, "true");
      PSModel doubleDenseModel = new PSModel(doubleDenseMatrix);
      doubleDenseModel.setSavePath(path);
      addPSModel("DoubleDense", doubleDenseModel);
      // angelClient.addMatrix(doubleDenseMatrix);

      MatrixContext doubleSparseMatrix = new MatrixContext();
      doubleSparseMatrix.setName("DoubleSparse");
      doubleSparseMatrix.setRowNum(1);
      doubleSparseMatrix.setColNum(100);
      doubleSparseMatrix.setMaxRowNumInBlock(1);
      doubleSparseMatrix.setMaxColNumInBlock(100 / psNumber);
      doubleSparseMatrix.setRowType(MLProtos.RowType.T_DOUBLE_SPARSE);
      doubleSparseMatrix.set(MatrixConfiguration.MATRIX_OPLOG_TYPE, "DENSE_DOUBLE");
      doubleSparseMatrix.set(MatrixConfiguration.MATRIX_AVERAGE, "true");
      PSModel doubleSparseModel = new PSModel(doubleSparseMatrix);
      doubleSparseModel.setSavePath(path);
      addPSModel("DoubleSparse", doubleSparseModel);
      // angelClient.addMatrix(doubleSparse);


      MatrixContext floatDenseMatrix = new MatrixContext();
      floatDenseMatrix.setName("FloatDense");
      floatDenseMatrix.setRowNum(1);
      floatDenseMatrix.setColNum(100);
      floatDenseMatrix.setMaxRowNumInBlock(1);
      floatDenseMatrix.setMaxColNumInBlock(100 / psNumber);
      floatDenseMatrix.setRowType(MLProtos.RowType.T_FLOAT_DENSE);
      floatDenseMatrix.set(MatrixConfiguration.MATRIX_OPLOG_TYPE, "DENSE_FLOAT");
      floatDenseMatrix.set(MatrixConfiguration.MATRIX_AVERAGE, "true");
      PSModel floatDenseModel = new PSModel(floatDenseMatrix);
      floatDenseModel.setSavePath(path);
      addPSModel("FloatDense", floatDenseModel);
      // angelClient.addMatrix(floatDenseMatrix);

      MatrixContext floatSparse = new MatrixContext();
      floatSparse.setName("FloatSparse");
      floatSparse.setRowNum(1);
      floatSparse.setColNum(100);
      floatSparse.setMaxRowNumInBlock(1);
      floatSparse.setMaxColNumInBlock(100 / psNumber);
      floatSparse.setRowType(MLProtos.RowType.T_FLOAT_SPARSE);
      floatSparse.set(MatrixConfiguration.MATRIX_OPLOG_TYPE, "DENSE_FLOAT");
      floatSparse.set(MatrixConfiguration.MATRIX_AVERAGE, "true");
      PSModel floatSparseModel = new PSModel(floatSparse);
      floatSparseModel.setSavePath(path);
      addPSModel("FloatSparse", floatSparseModel);
      // angelClient.addMatrix(floatSparse);

      MatrixContext intDenseMatrix = new MatrixContext();
      intDenseMatrix.setName("IntDense");
      intDenseMatrix.setRowNum(1);
      intDenseMatrix.setColNum(100);
      intDenseMatrix.setMaxRowNumInBlock(1);
      intDenseMatrix.setMaxColNumInBlock(100 / psNumber);
      intDenseMatrix.setRowType(MLProtos.RowType.T_INT_DENSE);
      intDenseMatrix.set(MatrixConfiguration.MATRIX_OPLOG_TYPE, "DENSE_INT");
      intDenseMatrix.set(MatrixConfiguration.MATRIX_AVERAGE, "true");
      PSModel intDenseModel = new PSModel(intDenseMatrix);
      intDenseModel.setSavePath(path);
      addPSModel("IntDense", intDenseModel);
      // angelClient.addMatrix(intDenseMatrix);

      MatrixContext intSparse = new MatrixContext();
      intSparse.setName("IntSparse");
      intSparse.setRowNum(1);
      intSparse.setColNum(100);
      intSparse.setMaxRowNumInBlock(1);
      intSparse.setMaxColNumInBlock(100 / psNumber);
      intSparse.setRowType(MLProtos.RowType.T_INT_SPARSE);
      intSparse.set(MatrixConfiguration.MATRIX_OPLOG_TYPE, "DENSE_INT");
      intSparse.set(MatrixConfiguration.MATRIX_AVERAGE, "true");
      PSModel intSparseModel = new PSModel(intSparse);
      intSparseModel.setSavePath(path);
      addPSModel("IntSparse", intSparseModel);
      // angelClient.addMatrix(intSparse);

      MatrixContext intArbitrary = new MatrixContext();
      intArbitrary.setName("IntArbitrary");
      intArbitrary.setRowNum(2);
      intArbitrary.setColNum(100);
      intArbitrary.setMaxRowNumInBlock(2);
      intArbitrary.setMaxColNumInBlock(100 / psNumber);
      intArbitrary.setRowType(MLProtos.RowType.T_INT_ARBITRARY);
      intArbitrary.set(MatrixConfiguration.MATRIX_OPLOG_TYPE, "LIL_INT");
      intArbitrary.set(MatrixConfiguration.MATRIX_AVERAGE, "true");
      PSModel intArbitraryModel = new PSModel(intArbitrary);
      intArbitraryModel.setSavePath(path);
      addPSModel("IntArbitrary", intArbitraryModel);
      // angelClient.addMatrix(intArbitrary);

    }

  }

  @Test
  public void testModelWriteTask() throws Exception {
    Configuration conf = new Configuration();

    //basic conf
    conf.setBoolean("mapred.mapper.new-api", true);
    conf.setBoolean(AngelConfiguration.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST, true);

    // use local deploy mode and dummy dataspliter
    conf.set(AngelConfiguration.ANGEL_DEPLOY_MODE, "LOCAL");
    conf.setBoolean(AngelConfiguration.ANGEL_AM_USE_DUMMY_DATASPLITER, false);
    // conf.setInt(AngelConfiguration.ANGEL_PREPROCESS_VECTOR_MAXDIM, 10000);
    conf.set(AngelConfiguration.ANGEL_INPUTFORMAT_CLASS, CombineTextInputFormat.class.getName());
    conf.set(AngelConfiguration.ANGEL_SAVE_MODEL_PATH, LOCAL_FS + TMP_PATH + "/out");
    conf.set(FileInputFormat.INPUT_DIR, "./src/test/data/itemMat");
    conf.set(AngelConfiguration.ANGEL_TASK_USER_TASKCLASS, ModelWriteTask.class.getName());
    angelClient = AngelClientFactory.get(conf);
    int psNumber =
            conf.getInt(AngelConfiguration.ANGEL_PS_NUMBER, AngelConfiguration.DEFAULT_ANGEL_PS_NUMBER);

    AlgorithmModel writeModel =
            new WriteModel(psNumber, conf.get(AngelConfiguration.ANGEL_SAVE_MODEL_PATH));

    conf.setInt(AngelConfiguration.ANGEL_WORKERGROUP_NUMBER, 1);
    conf.setInt(AngelConfiguration.ANGEL_PS_NUMBER, 1);
    conf.setInt(AngelConfiguration.ANGEL_WORKER_TASK_NUMBER, 1);

    angelClient.submit();
    angelClient.loadModel(writeModel);
    angelClient.start();
    angelClient.waitForCompletion();
    angelClient.saveModel(writeModel);
    angelClient.stop();
    LOG.info("stop local cluster");
  }


  @After
  public void tearDown() throws Exception {}
}

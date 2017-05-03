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

package com.tencent.angel;

import com.tencent.angel.client.AngelClient;
import com.tencent.angel.client.AngelClientFactory;
import com.tencent.angel.conf.AngelConfiguration;
import com.tencent.angel.conf.MatrixConfiguration;
import com.tencent.angel.example.ModelParseTask;
import com.tencent.angel.ml.algorithm.utils.ModelParse;
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
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ModelParseTest {
  private static final Log LOG = LogFactory.getLog(ModelParseTest.class);
  private static final String LOCAL_FS = LocalFileSystem.DEFAULT_FS;
  private static final String TMP_PATH = System.getProperty("java.io.tmpdir", "/tmp");
  private AngelClient angelClient;

  static {
    PropertyConfigurator.configure("../log4j.properties");
  }

  public class ParseModel extends AlgorithmModel {

    @Override
    public Storage<PredictResult> predict(Storage<LabeledData> storage) {
      return null;
    }

    public ParseModel(int psNumber, String path) {
      MatrixContext mMatrix = new MatrixContext();
      mMatrix.setName("test");
      mMatrix.setRowNum(1);
      mMatrix.setColNum(100);
      mMatrix.setMaxRowNumInBlock(1);
      mMatrix.setMaxColNumInBlock(100 / psNumber);
      mMatrix.setRowType(MLProtos.RowType.T_DOUBLE_DENSE);
      mMatrix.set(MatrixConfiguration.MATRIX_AVERAGE, "true");
      PSModel mModel = new PSModel(mMatrix);
      mModel.setSavePath(path);
      addPSModel("DoubleDense", mModel);
    }
  }


  @Test
  public void testInit() throws Exception {

  }

  @Test
  public void testLoadVector() throws Exception {
    String inputStr = "./src/test/data/itemMat";
    String outputStr = "./src/test/data";
    String matrixName = "itemMat";
    int convertThreadCount = 4;
    ModelParse modelLoader = new ModelParse(inputStr, outputStr, matrixName, convertThreadCount);
    modelLoader.convertModel();
  }


  @Test
  public void testModelParseTask() throws Exception {
    Configuration conf = new Configuration();
    conf.set(AngelConfiguration.ANGEL_PARSE_MODEL_PATH, "./src/test/data");
    conf.set(AngelConfiguration.ANGEL_MODEL_PARSE_NAME, "itemMat");
    conf.set(AngelConfiguration.ANGEL_MODEL_PARSE_THREAD_COUNT, "3");

    conf.setBoolean("mapred.mapper.new-api", true);
    conf.setBoolean(AngelConfiguration.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST, true);

    // use local deploy mode and dummy dataspliter
    conf.set(AngelConfiguration.ANGEL_DEPLOY_MODE, "LOCAL");
    conf.setBoolean(AngelConfiguration.ANGEL_AM_USE_DUMMY_DATASPLITER, false);
    // conf.setInt(AngelConfiguration.ANGEL_PREPROCESS_VECTOR_MAXDIM, 10000);
    conf.set(AngelConfiguration.ANGEL_INPUTFORMAT_CLASS, CombineTextInputFormat.class.getName());
    conf.set(AngelConfiguration.ANGEL_SAVE_MODEL_PATH, LOCAL_FS + TMP_PATH + "/out");
    conf.set(FileInputFormat.INPUT_DIR, "./src/test/data/IntArbitrary");
    conf.set(AngelConfiguration.ANGEL_TASK_USER_TASKCLASS, ModelParseTask.class.getName());
    angelClient = AngelClientFactory.get(conf);
    int psNumber =
            conf.getInt(AngelConfiguration.ANGEL_PS_NUMBER, AngelConfiguration.DEFAULT_ANGEL_PS_NUMBER);

    AlgorithmModel parseModel =
            new ParseModel(psNumber, conf.get(AngelConfiguration.ANGEL_SAVE_MODEL_PATH));
    conf.setInt(AngelConfiguration.ANGEL_WORKERGROUP_NUMBER, 1);
    conf.setInt(AngelConfiguration.ANGEL_PS_NUMBER, 1);
    conf.setInt(AngelConfiguration.ANGEL_WORKER_TASK_NUMBER, 1);

    angelClient.submit();
    angelClient.loadModel(parseModel);
    angelClient.start();
    angelClient.waitForCompletion();
    angelClient.saveModel(parseModel);
    angelClient.stop();
    LOG.info("stop local cluster");

  }


  @After
  public void tearDown() throws Exception {}
}

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

package com.tencent.angel.example;

import com.tencent.angel.AppSubmitter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;


public class WordCountSubmitter implements AppSubmitter {
  private static final Log LOG = LogFactory.getLog(WordCountSubmitter.class);
  public static final String parameterName = "weight.lr.sparse";
  public static final int totalSample = 5390283;
  public static final int dimension = 100000000;
  public static final int nonzero = 100;
  public static final String matrixNamePrefix = "w_";

  @Override
  public void submit(Configuration conf) throws Exception {
//    int psNumber =
//        conf.getInt(AngelConfiguration.ANGEL_PS_NUMBER, AngelConfiguration.DEFAULT_ANGEL_PS_NUMBER);
//
//    AngelClient jobClient = AngelClientFactory.get(conf);
//    jobClient.submit();
//    GetRowModel model = new GetRowModel();
//
//    int feaNum =
//        conf.getInt(AngelConfiguration.ANGEL_PREPROCESS_VECTOR_MAXDIM,
//            AngelConfiguration.DEFAULT_ANGEL_PREPROCESS_VECTOR_MAXDIM);
//
//    int factor = feaNum / (5000000 * psNumber);
//    int maxColInBlock = feaNum / (psNumber * (factor + 1));
//
//    LOG.info("factor=" + factor + ",maxColInBlock=" + maxColInBlock);
//    int rowNum =
//        conf.getInt(AngelConfiguration.ANGEL_TEST_MATRIX_ROWNUMBER,
//            AngelConfiguration.DEFAULT_ANGEL_TEST_MATRIX_ROWNUMBER);
//
//    int matrixNum = conf.getInt("angel.test.matrix.number", 1);
//    for (int i = 0; i < matrixNum; i++) {
//      MatrixContext mMatrix = new MatrixContext();
//      mMatrix.setName(matrixNamePrefix + i);
//      mMatrix.setRowNum(rowNum);
//      mMatrix.setColNum(feaNum);
//      mMatrix.setMaxRowNumInBlock(1);
//      mMatrix.setMaxColNumInBlock(maxColInBlock);
//      mMatrix.setRowType(MLProtos.RowType.T_DOUBLE_DENSE);
//      mMatrix.set(MatrixConfiguration.MATRIX_OPLOG_ENABLEFILTER, "false");
//      mMatrix.set(MatrixConfiguration.MATRIX_HOGWILD, "true");
//      mMatrix.set(MatrixConfiguration.MATRIX_AVERAGE, "false");
//      mMatrix.set(MatrixConfiguration.MATRIX_OPLOG_TYPE, "DENSE_DOUBLE");
//
//      model.addPSModel(new PSModel<>(mMatrix));
//    }
//
//    jobClient.loadModel(model);
//
//    jobClient.start();
//    jobClient.waitForCompletion();
//    jobClient.saveModel(model);
//    jobClient.stop();
  }
}

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

package com.tencent.angel.example.getValueOfIndex;


import com.tencent.angel.AppSubmitter;
import com.tencent.angel.client.AngelClient;
import com.tencent.angel.client.AngelClientFactory;
import com.tencent.angel.conf.MatrixConf;
import com.tencent.angel.ml.conf.MLConf;
import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.ml.matrix.RowType;
import org.apache.hadoop.conf.Configuration;

public class GetValueOfIndexSubmmiter implements AppSubmitter {
  public static String DENSE_DOUBLE_MAT = "DENSE_DOUBLE_MAT";
  public static String SPARSE_DOUBLE_MAT = "SPARSE_DOUBLE_MAT";
  public static String LONG_SPARSE_DOUBLE_MAT = "LONG_SPARSE_DOUBLE_MAT";


  @Override
  public void submit(Configuration conf) throws Exception {
    AngelClient client = AngelClientFactory.get(conf);
    int feaNum = conf.getInt(MLConf.ML_FEATURE_INDEX_RANGE(), MLConf.DEFAULT_ML_FEATURE_INDEX_RANGE());

    MatrixContext dMat = new MatrixContext(DENSE_DOUBLE_MAT, 1, feaNum, -1,-1);
    dMat.setRowType(RowType.T_DOUBLE_DENSE);
    dMat.set(MatrixConf.MATRIX_AVERAGE, "true");

    MatrixContext sMat = new MatrixContext(SPARSE_DOUBLE_MAT, 1, feaNum, -1,-1);
    sMat.setRowType(RowType.T_DOUBLE_SPARSE);
    sMat.set(MatrixConf.MATRIX_AVERAGE, "true");

    MatrixContext lMat = new MatrixContext(LONG_SPARSE_DOUBLE_MAT, 1, feaNum, -1,-1);
    lMat.setRowType(RowType.T_DOUBLE_SPARSE_LONGKEY);
    lMat.set(MatrixConf.MATRIX_AVERAGE, "true");


    client.addMatrix(dMat);
    client.addMatrix(sMat);
    client.addMatrix(lMat);

    client.startPSServer();
    client.run();
    client.waitForCompletion();
    client.stop(0);
  }
}

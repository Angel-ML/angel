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


package com.tencent.angel.worker;

import com.tencent.angel.AppSubmitter;
import com.tencent.angel.client.AngelClient;
import com.tencent.angel.client.AngelClientFactory;
import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.ml.matrix.RowType;
import org.apache.hadoop.conf.Configuration;

public class IndexGetRowTest implements AppSubmitter {
  public static String DENSE_DOUBLE_MAT = "dense_double_mat";
  public static String DENSE_DOUBLE_MAT_COMP = "dense_double_mat_comp";
  public static String SPARSE_DOUBLE_MAT = "sparse_double_mat";
  public static String SPARSE_DOUBLE_MAT_COMP = "sparse_double_mat_comp";

  public volatile static int rowNum = 1;
  public volatile static int colNum = 1000000;
  public volatile static int nnz = 1000000;
  public volatile static int blockRowNum = 1;
  public volatile static int blockColNum = 1000000;

  @Override public void submit(Configuration conf) throws Exception {
    AngelClient angelClient = AngelClientFactory.get(conf);

    rowNum = conf.getInt("row", 1);
    colNum = conf.getInt("col", 1000000);
    nnz = conf.getInt("nnz", 1000000);
    blockRowNum = conf.getInt("blockRow", 1);
    blockColNum = conf.getInt("blockCol", 1000000);

    // add dense double matrix
    MatrixContext dMat = new MatrixContext();
    dMat.setName(DENSE_DOUBLE_MAT);
    dMat.setRowNum(rowNum);
    dMat.setColNum(colNum);
    dMat.setMaxRowNumInBlock(blockRowNum);
    dMat.setMaxColNumInBlock(blockColNum);
    dMat.setRowType(RowType.T_DOUBLE_DENSE);
    angelClient.addMatrix(dMat);

    angelClient.startPSServer();
    angelClient.run();
    angelClient.waitForCompletion();
    angelClient.stop(0);
  }
}
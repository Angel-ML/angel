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


package com.tencent.angel.example.psf;

import com.tencent.angel.AppSubmitter;
import com.tencent.angel.client.AngelClient;
import com.tencent.angel.client.AngelClientFactory;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.ml.matrix.RowType;
import org.apache.hadoop.conf.Configuration;

public class SparseDoubleSubmit implements AppSubmitter {
  @Override public void submit(Configuration conf) throws Exception {
    conf.setBoolean(AngelConf.ANGEL_AM_USE_DUMMY_DATASPLITER, true);
    AngelClient angelClient = AngelClientFactory.get(conf);
    int blockCol = conf.getInt("blockcol", 5000000);
    MatrixContext context = new MatrixContext("sparse_double_test", 1, 2100000000, 1, blockCol);
    context.setRowType(RowType.T_DOUBLE_SPARSE);
    angelClient.addMatrix(context);
    angelClient.startPSServer();
    angelClient.run();
    angelClient.waitForCompletion();
    angelClient.stop(0);
  }
}
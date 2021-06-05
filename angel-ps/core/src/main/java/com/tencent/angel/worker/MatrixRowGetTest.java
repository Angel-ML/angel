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
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.conf.MatrixConf;
import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.ml.matrix.RowType;
import org.apache.hadoop.conf.Configuration;

public class MatrixRowGetTest implements AppSubmitter {

    public static String SPARSE_LONGKEY_FLOAT_MAT = "sparse_longkey_float_mat";

    public volatile static int rowNum = 1;
    //public volatile static int colNum = 1000000;
    public volatile static int colNum = 10;
    public volatile static long longcolNum = Long.MAX_VALUE;

    //public volatile static int nnz = 10000;
    public volatile static int nnz = 2;
    public volatile static int longnnz = 100000000;
    public volatile static int nGet = 300000;

    public volatile static int blockRowNum = 1;
    //public volatile static int blockColNum = 20000;
    public volatile static int blockColNum = 10;
    public volatile static long longBlockColNum = longcolNum / 200;

    @Override public void submit(Configuration conf) throws Exception {
        conf.setBoolean(AngelConf.ANGEL_AM_USE_DUMMY_DATASPLITER, true);
        AngelClient angelClient = AngelClientFactory.get(conf);

        // test the colNum is 1000000, 1 million
//        rowNum = conf.getInt("row", 1);
//        colNum = conf.getInt("col", 1000000);
//        nnz = conf.getInt("nnz", 10000);
//        blockRowNum = conf.getInt("blockRow", 1);
//        blockColNum = conf.getInt("blockCol", 20000);

//        rowNum = conf.getInt("row", 1);
//        colNum = conf.getInt("col", 10);
//        nnz = conf.getInt("nnz", 2);
//        blockRowNum = conf.getInt("blockRow", 1);
//        blockColNum = conf.getInt("blockCol", 10);

        // add sparse longkey float matrix of 1 million
//        MatrixContext dMat = new MatrixContext();
//        dMat.setName(SPARSE_LONGKEY_FLOAT_MAT);
//        dMat.setRowNum(rowNum);
//        dMat.setColNum(colNum);
//        dMat.setMaxRowNumInBlock(blockRowNum);
//        dMat.setMaxColNumInBlock(blockColNum);
//        dMat.setRowType(RowType.T_FLOAT_SPARSE_LONGKEY);
//        dMat.set(MatrixConf.MATRIX_SAVE_PATH, conf.get("angel.save.model.path"));
//        angelClient.addMatrix(dMat);

        // test the colNum is Long.MAX_VALUE
        rowNum = conf.getInt("row", 1);
        longcolNum = conf.getLong("col", Long.MAX_VALUE);
        longnnz = conf.getInt("nnz", 100000000);
        blockRowNum = conf.getInt("blockRow", 1);
        longBlockColNum = conf.getLong("blockCol", Long.MAX_VALUE / 200);

        // add sparse longkey float matrix of Long.MAX_VALUE
        MatrixContext longMat = new MatrixContext();
        longMat.setName(SPARSE_LONGKEY_FLOAT_MAT);
        longMat.setRowNum(rowNum);
        longMat.setColNum(longcolNum);
        longMat.setMaxRowNumInBlock(blockRowNum);
        longMat.setMaxColNumInBlock(longBlockColNum);
        longMat.setRowType(RowType.T_FLOAT_SPARSE_LONGKEY);
        angelClient.addMatrix(longMat);

        angelClient.startPSServer();
        angelClient.run();
        angelClient.waitForCompletion();
        angelClient.stop(0);
    }
}


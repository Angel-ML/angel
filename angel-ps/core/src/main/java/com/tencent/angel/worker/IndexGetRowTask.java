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

import com.tencent.angel.exception.AngelException;
import com.tencent.angel.exception.InvalidParameterException;
import com.tencent.angel.ml.math2.VFactory;
import com.tencent.angel.ml.math2.vector.IntDoubleVector;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.psf.get.getrow.GetRowResult;
import com.tencent.angel.ml.matrix.psf.get.indexed.IndexGet;
import com.tencent.angel.ml.matrix.psf.get.indexed.IndexGetParam;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.MatrixClient;
import com.tencent.angel.worker.task.BaseTask;
import com.tencent.angel.worker.task.TaskContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Random;

public class IndexGetRowTask extends BaseTask<Long, Long, Long> {
  private static final Log LOG = LogFactory.getLog(IndexGetRowTask.class);

  public IndexGetRowTask(TaskContext taskContext) {
    super(taskContext);
  }

  @Override public void preProcess(TaskContext taskContext) {
  }

  @Override public Long parse(Long key, Long value) {
    return null;
  }

  @Override public void run(TaskContext taskContext) throws AngelException {
    try {
      MatrixClient matrixClient = taskContext.getMatrix(IndexGetRowTest.DENSE_DOUBLE_MAT);
      MatrixMeta matrixMeta =
        PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(IndexGetRowTest.DENSE_DOUBLE_MAT);

      int[] indices = genIndexs((int) matrixMeta.getColNum(), IndexGetRowTest.nnz);

      IntDoubleVector delta =
        VFactory.sparseDoubleVector((int) matrixMeta.getColNum(), IndexGetRowTest.nnz);
      for (int i = 0; i < IndexGetRowTest.nnz; i++) {
        delta.set(indices[i], indices[i]);
      }

      IntDoubleVector delta1 = VFactory.denseDoubleVector((int) matrixMeta.getColNum());
      for (int i = 0; i < IndexGetRowTest.colNum; i++) {
        delta1.set(i, i);
      }

      LOG.info("delta use " + delta.getType() + " type storage");

      int testNum = 500;
      long startTs = System.currentTimeMillis();
      LOG.info("for sparse delta type");
      conf.setBoolean("use.new.split", false);
      for (int i = 0; i < testNum; i++) {
        matrixClient.increment(0, delta, true);
        if (i > 0 && i % 10 == 0) {
          LOG.info("increment old use time = " + (System.currentTimeMillis() - startTs) / i);
        }
      }

      conf.setBoolean("use.new.split", true);
      startTs = System.currentTimeMillis();
      for (int i = 0; i < testNum; i++) {
        matrixClient.increment(0, delta, true);
        //IntDoubleVector vector = (IntDoubleVector) ((GetRowResult) matrixClient.get(func)).getRow();
        if (i > 0 && i % 10 == 0) {
          LOG.info("increment new use time = " + (System.currentTimeMillis() - startTs) / i);
        }
      }

      LOG.info("for dense delta type");
      conf.setBoolean("use.new.split", false);
      for (int i = 0; i < testNum; i++) {
        matrixClient.increment(0, delta1, true);
        if (i > 0 && i % 10 == 0) {
          LOG.info("increment old use time = " + (System.currentTimeMillis() - startTs) / i);
        }
      }

      conf.setBoolean("use.new.split", true);
      startTs = System.currentTimeMillis();
      for (int i = 0; i < testNum; i++) {
        matrixClient.increment(0, delta1, true);
        //IntDoubleVector vector = (IntDoubleVector) ((GetRowResult) matrixClient.get(func)).getRow();
        if (i > 0 && i % 10 == 0) {
          LOG.info("increment new use time = " + (System.currentTimeMillis() - startTs) / i);
        }
      }
    } catch (InvalidParameterException e) {
      LOG.error("get matrix failed ", e);
      throw new AngelException(e);
    }
  }

  public static int[] genIndexs(int feaNum, int nnz) {
    int[] sortedIndex = new int[nnz];
    Random random = new Random(System.currentTimeMillis());
    sortedIndex[0] = random.nextInt(feaNum / nnz);
    for (int i = 1; i < nnz; i++) {
      int rand = random.nextInt((feaNum - sortedIndex[i - 1]) / (nnz - i));
      if (rand == 0)
        rand = 1;
      sortedIndex[i] = rand + sortedIndex[i - 1];
    }

    return sortedIndex;
  }
}

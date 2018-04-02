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

import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ml.conf.MLConf;
import com.tencent.angel.ml.math.vector.DenseDoubleVector;
import com.tencent.angel.ml.math.vector.SparseDoubleVector;
import com.tencent.angel.ml.matrix.psf.get.enhance.indexed.IndexGetFunc;
import com.tencent.angel.ml.matrix.psf.get.enhance.indexed.IndexGetParam;
import com.tencent.angel.ml.matrix.psf.get.single.GetRowResult;
import com.tencent.angel.psagent.matrix.MatrixClient;
import com.tencent.angel.worker.task.BaseTask;
import com.tencent.angel.worker.task.TaskContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Random;

import static com.tencent.angel.example.getValueOfIndex.GetValueOfIndexSubmmiter.DENSE_DOUBLE_MAT;
import static com.tencent.angel.example.getValueOfIndex.GetValueOfIndexSubmmiter.SPARSE_DOUBLE_MAT;

public class GetValueOfIndexTask extends BaseTask<Long, Long, Long> {
  Log LOG = LogFactory.getLog(GetValueOfIndexTask.class);

  int feaNum = conf.getInt(MLConf.ML_FEATURE_INDEX_RANGE(), MLConf.DEFAULT_ML_FEATURE_INDEX_RANGE());
  int nnz = conf.getInt(MLConf.ML_MODEL_SIZE(), MLConf.DEFAULT_ML_MODEL_SIZE());

  public GetValueOfIndexTask(TaskContext taskContext) {super(taskContext);}

  @Override public void preProcess(TaskContext taskContext) { }

  @Override
  public Long parse(Long key, Long value) {return null;}

  @Override
  public void run(TaskContext tContext) throws AngelException {
    LOG.info("Feature Number=" + feaNum + " nnz=" + nnz);
    LOG.info("Dense Double Vector.");
    denseDouble(tContext);

    LOG.info("Sparse Double Vector");
    sparseDouble(tContext);
  }

  public void denseDouble(TaskContext tContext) {
    long startGen = System.currentTimeMillis();
    int[] index = genIndexs(feaNum, nnz);
    long cost = System.currentTimeMillis() - startGen;
    LOG.info("Gen index cost: " + cost + " ms.");

    try {
      MatrixClient dMatClient = tContext.getMatrix(DENSE_DOUBLE_MAT);

      // Set PS Model values
      long startInc = System.currentTimeMillis();
      DenseDoubleVector delt = new DenseDoubleVector(feaNum);
      for (int i = 0; i < feaNum; i++)
        delt.set(i, (double) i);
      dMatClient.increment(0, delt);
      dMatClient.clock().get();
      long costInc = System.currentTimeMillis() - startInc;
      LOG.info("Increment delt cost " + costInc + " ms.");

      // Wait for all tasks finish this clock
      dMatClient.getTaskContext().globalSync(dMatClient.getMatrixId());

      // Get values of index array
      long startGet = System.currentTimeMillis();
      IndexGetFunc func = new IndexGetFunc(new IndexGetParam(tContext
          .getMatrix(DENSE_DOUBLE_MAT).getMatrixId(), 0, index));
      SparseDoubleVector row = (SparseDoubleVector) ((GetRowResult) dMatClient.get(func)).getRow();
      long costGet = System.currentTimeMillis() - startGet;
      LOG.info("Get row of indexs cost " + costGet + " ms.");

    } catch (Throwable e) {
      throw new AngelException(e);
    }

  }

  public void sparseDouble(TaskContext tContext) {
    long startGen = System.currentTimeMillis();
    int[] index = genIndexs(feaNum, nnz);
    long cost = System.currentTimeMillis() - startGen;
    LOG.info("Gen index cost: " + cost + " ms.");

    try {
      MatrixClient sMatClient = tContext.getMatrix(SPARSE_DOUBLE_MAT);

      // Set PS Model values
      long startInc = System.currentTimeMillis();
      SparseDoubleVector delt = new SparseDoubleVector(feaNum);
      for (int i = 0; i < feaNum; i++)
        delt.set(i, (double) i);
      sMatClient.increment(0, delt);
      sMatClient.clock().get();
      long costInc = System.currentTimeMillis() - startInc;
      LOG.info("Increment delt cost " + costInc + " ms.");

      // Wait for all tasks finish this clock
      sMatClient.getTaskContext().globalSync(sMatClient.getMatrixId());

      // Get values of index array
      long startGet = System.currentTimeMillis();
      IndexGetFunc func = new IndexGetFunc(new IndexGetParam(tContext
          .getMatrix(DENSE_DOUBLE_MAT).getMatrixId(), 0, index));
      SparseDoubleVector row = (SparseDoubleVector) ((GetRowResult) sMatClient.get(func)).getRow();
      long costGet = System.currentTimeMillis() - startGet;
      LOG.info("Get row of indexs cost " + costGet + " ms.");

    } catch (Throwable e) {
      throw new AngelException(e);
    }

  }

  public static int[] genIndexs(int feaNum, int nnz) {

    int[] sortedIndex = new int[nnz];
    Random random = new Random(System.currentTimeMillis());
    sortedIndex[0] = random.nextInt(feaNum/nnz);
    for (int i = 1; i < nnz; i ++) {
      int rand = random.nextInt( (feaNum - sortedIndex[i-1]) / (nnz - i) );
      if (rand==0) rand = 1;
      sortedIndex[i] = rand + sortedIndex[i-1];
    }

    return sortedIndex;
  }

}

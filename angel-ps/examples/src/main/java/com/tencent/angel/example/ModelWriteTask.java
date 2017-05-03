
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

import com.tencent.angel.ml.feature.LabeledData;
import com.tencent.angel.ml.math.vector.*;
import com.tencent.angel.psagent.matrix.MatrixClient;
import com.tencent.angel.worker.task.TaskContext;
import com.tencent.angel.worker.task.TrainTask;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Random;

public class ModelWriteTask extends TrainTask<LongWritable, Text> {
  private static final Log LOG = LogFactory.getLog(ModelWriteTask.class);


  public ModelWriteTask(TaskContext taskContext) throws IOException {
    super(taskContext);
  }


  @Override
  public LabeledData parse(LongWritable key, Text value) {
    return null;
  }

  @Override
  public void run(TaskContext taskContext) throws Exception {
    double doubleValue[] = new double[100];
    for (int i = 0; i < doubleValue.length; i++)
      doubleValue[i] = i + 0.1;
    DenseDoubleVector denseDoubleVector = new DenseDoubleVector(doubleValue.length, doubleValue);
    MatrixClient doubleDenseClient = taskContext.getMatrix("DoubleDense");
    denseDoubleVector.setMatrixId(doubleDenseClient.getMatrixId());
    denseDoubleVector.setRowId(0);
    doubleDenseClient.increment(denseDoubleVector);
    doubleDenseClient.increment(denseDoubleVector);
    doubleDenseClient.clock().get();

    float floatValue[] = new float[100];
    for (int i = 0; i < floatValue.length; i++)
      floatValue[i] = i + 0.1f;
    DenseFloatVector denseFloatVector = new DenseFloatVector(floatValue.length, floatValue);
    MatrixClient floatDenseClient = taskContext.getMatrix("FloatDense");
    denseFloatVector.setMatrixId(floatDenseClient.getMatrixId());
    denseFloatVector.setRowId(0);
    floatDenseClient.increment(denseFloatVector);
    floatDenseClient.clock().get();

    int intValue[] = new int[100];
    for (int i = 0; i < intValue.length; i++)
      intValue[i] = i;
    DenseIntVector denseIntVector = new DenseIntVector(intValue.length, intValue);
    MatrixClient intDenseClient = taskContext.getMatrix("IntDense");
    denseIntVector.setMatrixId(intDenseClient.getMatrixId());
    denseIntVector.setRowId(0);
    intDenseClient.increment(denseIntVector);
    intDenseClient.clock().get();

    int indics[] = new int[100];
    Random random = new Random();
    for (int i = indics.length - 1; i > 0; i--) {
      indics[i] = random.nextInt(i + 1);
      indics[i] = i;
    }

    SparseDoubleVector sparseDoubleVector =
            new SparseDoubleVector(indics.length, indics, doubleValue);
    MatrixClient doubleSparseClient = taskContext.getMatrix("DoubleSparse");
    sparseDoubleVector.setMatrixId(doubleSparseClient.getMatrixId());
    sparseDoubleVector.setRowId(0);
    doubleSparseClient.increment(sparseDoubleVector);
    doubleSparseClient.clock().get();

    SparseFloatVector sparseFloatVector = new SparseFloatVector(indics.length, indics, floatValue);
    MatrixClient floatSparseClient = taskContext.getMatrix("FloatSparse");
    sparseFloatVector.setMatrixId(floatSparseClient.getMatrixId());
    sparseFloatVector.setRowId(0);
    floatSparseClient.increment(sparseFloatVector);
    floatSparseClient.clock().get();

    SparseIntVector sparseIntVector = new SparseIntVector(indics.length, indics, intValue);
    MatrixClient intSparseClient = taskContext.getMatrix("IntSparse");
    sparseIntVector.setMatrixId(intSparseClient.getMatrixId());
    sparseIntVector.setRowId(0);
    intSparseClient.increment(sparseIntVector);
    intSparseClient.clock().get();

    MatrixClient intArbitraryClient = taskContext.getMatrix("IntArbitrary");
    DenseIntVector arbitraryDenseIntVector = new DenseIntVector(intValue.length, intValue);
    arbitraryDenseIntVector.setMatrixId(intArbitraryClient.getMatrixId());
    arbitraryDenseIntVector.setRowId(0);
    intArbitraryClient.increment(arbitraryDenseIntVector);
    int sparseIndics[] = {1, 2, 88};
    int sparseValues[] = {4, 5, 6};
    SparseIntVector arbitrarySparseIntVector =
            new SparseIntVector(sparseIndics.length, sparseIndics, sparseValues);
    arbitrarySparseIntVector.setMatrixId(intArbitraryClient.getMatrixId());
    arbitrarySparseIntVector.setRowId(1);
    intArbitraryClient.increment(arbitrarySparseIntVector);
    intArbitraryClient.clock().get();


  }

}

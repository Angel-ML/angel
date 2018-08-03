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

package com.tencent.angel.ml;

import com.tencent.angel.ml.math.vector.DenseDoubleVector;
import com.tencent.angel.ml.math.vector.SparseDummyVector;
import com.tencent.angel.ml.math.vector.SparseDoubleVector;
import com.tencent.angel.ml.math.vector.SparseIntVector;
import com.tencent.angel.utils.Time;
import junit.framework.Assert;
import org.junit.Test;

import java.util.Random;


public class DoubleVectorTest {

  @Test
  public void timesBySparseTest() {
    SparseDoubleVector vector = new SparseDoubleVector(10);
    vector.set(1, 1.0);
    vector.set(3, 3.0);
    vector.set(5, 5.0);

    vector.timesBy(2.0);

    Assert.assertEquals(2.0, vector.get(1), 0.0);
    Assert.assertEquals(6.0, vector.get(3), 0.0);
    Assert.assertEquals(10.0, vector.get(5), 0.0);
  }

  @Test
  public void plusBySparseTest() {
    SparseDoubleVector vector = new SparseDoubleVector(10);
    vector.set(1, 1.0);
    vector.set(3, 3.0);
    vector.set(5, 5.0);

    SparseDummyVector dummyVector = new SparseDummyVector(10);
    dummyVector.set(1, 1.0);
    dummyVector.set(3, 1.0);

    vector.plusBy(dummyVector, 1.0);

    Assert.assertEquals(2.0, vector.get(1), 0.0);
    Assert.assertEquals(4.0, vector.get(3), 0.0);
    Assert.assertEquals(5.0, vector.get(5), 0.0);
  }

  @Test
  public void densePlusBySparseTest() {
    DenseDoubleVector vector = new DenseDoubleVector(10);
    vector.set(1, 1.0);
    vector.set(3, 3.0);
    vector.set(5, 5.0);

    SparseDoubleVector sparseDoubleVector = new SparseDoubleVector(10);
    sparseDoubleVector.set(1, 1.0);
    sparseDoubleVector.set(3, 3.0);
    sparseDoubleVector.set(7, 7.0);

    vector.plusBy(sparseDoubleVector, 1.0);

    Assert.assertEquals(2.0, vector.get(1), 0.0);
    Assert.assertEquals(6.0, vector.get(3), 0.0);
    Assert.assertEquals(5.0, vector.get(5), 0.0);
    Assert.assertEquals(7.0, vector.get(7), 0.0);
  }

  @Test
  public void densePlusByDummyTest() {
    DenseDoubleVector vector = new DenseDoubleVector(10);
    vector.set(1, 1.0);
    vector.set(3, 3.0);
    vector.set(5, 5.0);

    SparseDummyVector dummyVector = new SparseDummyVector(10);
    dummyVector.set(1, 1.0);
    dummyVector.set(3, 1.0);

    vector.plusBy(dummyVector, 1.0);

    Assert.assertEquals(2.0, vector.get(1), 0.0);
    Assert.assertEquals(4.0, vector.get(3), 0.0);
    Assert.assertEquals(5.0, vector.get(5), 0.0);
    Assert.assertEquals(0.0, vector.get(7), 0.0);
  }

  @Test
  public void sparseIntVectorTest() {
    int dim = 1000;
    int[] indices = new int[dim];
    int[] values  = new int[dim];

    Random random = new Random(Time.monotonicNow());
    for (int i = 0; i < dim; i ++) {
      indices[i] = random.nextInt(dim);
      values[i]  = random.nextInt(dim);
    }

    SparseIntVector vector = new SparseIntVector(dim, indices, values);


    int[] indices2 = vector.getIndices();
    int[] values2  = vector.getValues();

    for (int i = 0; i < indices2.length; i ++) {
      Assert.assertEquals(vector.get(indices2[i]), values2[i]);
    }
  }


  @Test
  public void sparseDoubleVectorTest() {
    int dim = 1000;
    int[] indices = new int[dim];
    double[] values  = new double[dim];

    Random random = new Random(Time.monotonicNow());
    for (int i = 0; i < dim; i ++) {
      indices[i] = random.nextInt(dim);
      values[i]  = random.nextInt(dim);
    }

    SparseDoubleVector vector = new SparseDoubleVector(dim, indices, values);

    int[] indices2 = vector.getIndices();
    double[] values2  = vector.getValues();

    for (int i = 0; i < indices2.length; i ++) {
      Assert.assertEquals(vector.get(indices2[i]), values2[i], 0.0);
    }
  }

}

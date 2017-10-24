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

package com.tencent.angel.ml;

import com.tencent.angel.ml.math.vector.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

public class Utils {
  public static DenseIntDoubleVector genDenseDoubleVector(int dim) {
    Random random = new Random(System.currentTimeMillis());
    double[] value = new double[dim];

    DenseIntDoubleVector vec = new DenseIntDoubleVector(dim, value);

    return vec;
  }

  public static DenseFloatVector genDenseFloatVector(int dim) {
    float[] value = genFloatArray(dim);
    DenseFloatVector vec = new DenseFloatVector(dim, value);
    return vec;
  }

  public static SparseIntDoubleSortedVector genSparseDoubleSortedVector(int dim) {
    Random random = new Random(System.currentTimeMillis());
    int nnz = random.nextInt(dim);
    int[] indexs = genSortedIndexs(nnz, dim);
    double[] values = new double[nnz];
    for (int i = 0; i < nnz; i++)
      values[i] = random.nextDouble();

    SparseIntDoubleSortedVector vec = new SparseIntDoubleSortedVector(dim, indexs, values);

    return  vec;
  }


  public static SparseIntDoubleSortedVector genSparseDoubleSortedVector(int nnz, int dim) {
    Random random = new Random(System.currentTimeMillis());
    int[] indexs = genSortedIndexs(nnz, dim);
    double[] values = new double[nnz];
    for (int i = 0; i < nnz; i++)
      values[i] = random.nextDouble();

    SparseIntDoubleSortedVector vec = new SparseIntDoubleSortedVector(dim, indexs, values);

    return  vec;
  }

  public static SparseIntDoubleVector genSparseDoubleVector(int nnz, int dim) {
    int[] index = genSortedIndexs(nnz, dim);
    double[] values = genDoubleArray(nnz);

    SparseIntDoubleVector vec = new SparseIntDoubleVector(dim, index, values);
    return  vec;
  }

  public static SparseFloatVector genSparseFloatVector(int nnz, int dim) {
    int[] index = genSortedIndexs(nnz, dim);
    float[] values = genFloatArray(nnz);

    SparseFloatVector vec = new SparseFloatVector(dim, index, values);
    return  vec;
  }

  public static SparseDummyVector genSparseDummyVector(int nnz, int dim) {
    int[] index = genSortedIndexs(nnz, dim);

    SparseDummyVector vec = new SparseDummyVector(dim, nnz);

    for (int idx: index)
      vec.set(idx, 1);

    return vec;
  }

  public static SparseDummyVector genSparseDummyVector(int dim) {
    Random random = new Random(System.currentTimeMillis());
    int nnz = random.nextInt();

    int[] index = genSortedIndexs(nnz, dim);

    SparseDummyVector vec = new SparseDummyVector(dim, nnz);

    for (int idx: index)
      vec.set(idx, 1);

    return vec;
  }

  public static int[] genIndexs(int nnz, int dim) {
    ArrayList<Integer> indexList = new ArrayList<>();
    for (int i = 0; i < dim; i++)
      indexList.add(i);

    int[] indexs = new int[nnz];

    Random random = new Random(System.currentTimeMillis());
    for (int i = 0; i < nnz; i++) {
      int idx = indexList.get(random.nextInt(indexList.size()));
      indexs[i] = idx;
      indexList.remove(indexList.indexOf(idx));
    }

    return indexs;
  }

  public static int[] genSortedIndexs(int nnz, int dim) {
    int[] indexs = genIndexs(nnz, dim);
    Arrays.sort(indexs);
    return indexs;
  }

  public static double[] genDoubleArray(int dim) {
    Random random = new Random(System.currentTimeMillis());
    double[] array = new double[dim];
    for (int i = 0; i < dim; i++)
      array[i] = random.nextDouble();

    return array;
  }

  public static float[] genFloatArray(int dim) {
    Random random = new Random(System.currentTimeMillis());
    float[] array = new float[dim];
    for (int i = 0; i < dim; i++)
      array[i] = random.nextFloat();

    return array;
  }

}

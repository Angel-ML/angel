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

package com.tencent.angel.ml.math.matrix;

import com.tencent.angel.ml.math.vector.DenseIntVector;
import com.tencent.angel.ml.math.vector.TIntVector;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;

public class DenseIntMatrixTest {
  @Test
  public void inc() throws Exception {
    int[][] value = {{1, 2}, {3, 4}};
    DenseIntMatrix mat = new DenseIntMatrix(2, 2, value);

    mat.plusBy(0, 0, 1);
    mat.plusBy(1, 0, 2);

    assertEquals(2, mat.get(0, 0));
    assertEquals(2, mat.get(0, 1));
    assertEquals(5, mat.get(1, 0));
    assertEquals(4, mat.get(1, 1));
  }

  @Test
  public void plusBy() throws Exception {
    DenseIntMatrix mat = new DenseIntMatrix(2, 2, new int[][]{{1, 2}, {3, 4}});
    DenseIntMatrix mat_1 = new DenseIntMatrix(2, 2, new int[][]{{5, 6}, {7, 8}});

    mat.plusBy(mat_1);

    assertEquals(6, mat.get(0, 0));
    assertEquals(8, mat.get(0, 1));
    assertEquals(10, mat.get(1, 0));
    assertEquals(12, mat.get(1, 1));

  }

  @Test
  public void plusBy1() throws Exception {
    DenseIntMatrix mat = new DenseIntMatrix(2, 2, new int[][]{{1, 2}, {3, 4}});
    TIntMatrix mat_1 = new DenseIntMatrix(2, 2, new int[][]{{5, 6}, {7, 8}});

    mat.plusBy(mat_1);

    assertEquals(6, mat.get(0, 0));
    assertEquals(8, mat.get(0, 1));
    assertEquals(10, mat.get(1, 0));
    assertEquals(12, mat.get(1, 1));

  }

  @Test
  public void plusBy2() throws Exception {
    int[][] value = {{1, 2}, {3, 4}};
    DenseIntMatrix mat = new DenseIntMatrix(2, 2, value);
    DenseIntVector vec = new DenseIntVector(2, new int[]{1, 2});
    vec.setRowId(0);

    mat.plusBy(vec);

    assertEquals(2, mat.get(0, 0));
    assertEquals(4, mat.get(0, 1));
    assertEquals(3, mat.get(1, 0));
    assertEquals(4, mat.get(1, 1));

    DenseIntMatrix mat_1 = new DenseIntMatrix(2, 2);

    mat_1.plusBy(vec);

    assertEquals(1, mat_1.get(0, 0));
    assertEquals(2, mat_1.get(0, 1));
    assertEquals(0, mat_1.get(1, 0));
    assertEquals(0, mat_1.get(1, 1));

  }

  @Test
  public void plusBy3() throws Exception {
    int[][] value = {{1, 2}, {3, 4}};
    DenseIntMatrix mat = new DenseIntMatrix(2, 2, value);
    TIntVector vec = new DenseIntVector(2, new int[]{1, 2});
    vec.setRowId(0);

    mat.plusBy(vec);

    assertEquals(2, mat.get(0, 0));
    assertEquals(4, mat.get(0, 1));
    assertEquals(3, mat.get(1, 0));
    assertEquals(4, mat.get(1, 1));

    DenseIntMatrix mat_1 = new DenseIntMatrix(2, 2);

    mat_1.plusBy(vec);

    assertEquals(1, mat_1.get(0, 0));
    assertEquals(2, mat_1.get(0, 1));
    assertEquals(0, mat_1.get(1, 0));
    assertEquals(0, mat_1.get(1, 1));

  }

  @Test
  public void sparsity() throws Exception {
    int[][] value = {{1, 2}, {3, 4}};
    DenseIntMatrix mat = new DenseIntMatrix(2, 2, value);

    assertEquals(1.0, mat.sparsity());

    DenseIntMatrix mat_1 = new DenseIntMatrix(2, 2);

    assertEquals(0.0, mat_1.sparsity());

  }

  @Test
  public void size() throws Exception {
    int[][] value = {{1, 2}, {3, 4}};
    DenseIntMatrix mat = new DenseIntMatrix(2, 2, value);

    assertEquals(4, mat.size());

    DenseIntMatrix mat_1 = new DenseIntMatrix(2, 2);

    assertEquals(4, mat_1.size());

  }

  @Test
  public void getValues() throws Exception {
    int[][] value = {{1, 2}, {3, 4}};
    DenseIntMatrix mat = new DenseIntMatrix(2, 2, value);

    DenseIntVector row0 = (DenseIntVector)mat.getRow(0);
    DenseIntVector row1 = (DenseIntVector)mat.getRow(1);

    assertEquals(1, row0.get(0));
    assertEquals(2, row0.get(1));
    assertEquals(3, row1.get(0));
    assertEquals(4, row1.get(1));

  }

  @Test
  public void get() throws Exception {
    int[][] value = {{1, 2}, {3, 4}};
    DenseIntMatrix mat = new DenseIntMatrix(2, 2, value);

    assertEquals(1, mat.get(0, 0));
    assertEquals(2, mat.get(0, 1));
    assertEquals(3, mat.get(1, 0));
    assertEquals(4, mat.get(1, 1));

  }

  @Test
  public void getTIntVector() throws Exception {
    int[][] value = {{1, 2}, {3, 4}};
    DenseIntMatrix mat = new DenseIntMatrix(2, 2, value);
    TIntVector vec = (TIntVector)mat.getRow(0);

    assertEquals(1, vec.get(0));
    assertEquals(2, vec.get(1));

    DenseIntMatrix mat_1 = new DenseIntMatrix(2, 2);
    mat_1.plusBy(vec);
    TIntVector vec_1 = (TIntVector)mat_1.getRow(1);

    assertEquals(0, vec_1.get(0));
    assertEquals(0, vec_1.get(1));

  }

  @Test
  public void clear() throws Exception {
    DenseIntMatrix mat = new DenseIntMatrix(2, 2, new int[][]{{1, 2}, {3, 4}});

    mat.clear();

    assertEquals(0, mat.get(0, 0));
    assertEquals(0, mat.get(0, 1));
    assertEquals(0, mat.get(1, 0));
    assertEquals(0, mat.get(1, 1));

  }

  @Test
  public void nonZeroNum() throws Exception {
    DenseIntMatrix mat = new DenseIntMatrix(2, 2, new int[][]{{0, 2}, {0, 4}});

    assertEquals(2, mat.nonZeroNum());

    DenseIntMatrix mat_1 = new DenseIntMatrix(2, 2);
    DenseIntVector vec = new DenseIntVector(2, new int[]{1, 2});
    vec.setRowId(0);
    mat_1.plusBy(vec);

    assertEquals(2, mat_1.nonZeroNum());

  }

}

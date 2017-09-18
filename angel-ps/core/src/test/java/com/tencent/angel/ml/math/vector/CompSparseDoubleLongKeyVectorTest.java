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

package com.tencent.angel.ml.math.vector;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.utils.Sort;
import org.junit.Test;

import java.util.HashSet;
import java.util.Random;

import static junit.framework.Assert.assertEquals;

public class CompSparseDoubleLongKeyVectorTest {
  @Test
  public void testBaseOp(){
    CompSparseDoubleLongKeyVector vector = initVector();
    HashSet<Long> indexSet = new HashSet<Long>();
    Random r = new Random();
    int sampleNum = 100;
    while(true) {
      indexSet.add(r.nextLong());
      if(indexSet.size() >= sampleNum) {
        break;
      }
    }
    long [] indexes = new long[sampleNum];
    int index = 0;
    for(long item:indexSet) {
      indexes[index++] = item;
    }

    for(int i = 0; i < indexes.length; i++) {
      vector.plusBy(indexes[i], 1.0);
    }
    for(int i = 0; i < indexes.length; i++) {
      assertEquals(vector.get(indexes[i]), 1.0);
    }

    assertEquals(vector.sum(), Double.valueOf(sampleNum));
    assertEquals(vector.nonZeroNumber(), sampleNum);
    assertEquals(vector.squaredNorm(), Double.valueOf(sampleNum));
    vector.timesBy(2.0);
    assertEquals(vector.sum(), Double.valueOf(sampleNum) * 2);

    CompSparseDoubleLongKeyVector addVector = initVector();
    for(int i = 0; i < indexes.length; i++) {
      addVector.plusBy(indexes[i], 1.0);
    }

    vector.plusBy(addVector);
    for(int i = 0; i < indexes.length; i++) {
      assertEquals(vector.get(indexes[i]), 3.0);
    }

    assertEquals(addVector.dot(vector), Double.valueOf(sampleNum) * 3);

    SparseDummyLongKeyVector addVector1 = new SparseDummyLongKeyVector(-1);
    for(int i = 0; i < indexes.length; i++) {
      addVector1.set(indexes[i], 1.0);
    }
    vector.plusBy(addVector1);
    for(int i = 0; i < indexes.length; i++) {
      assertEquals(vector.get(indexes[i]), 4.0);
    }

    double [] values = new double[sampleNum];
    for(int i = 0; i < values.length; i++) {
      values[i] = 1.0;
    }
    Sort.quickSort(indexes, values, 0, sampleNum -  1);
    SparseDoubleLongKeySortedVector addVector2 = new SparseDoubleLongKeySortedVector(-1, indexes, values);
    vector.plusBy(addVector2);
    for(int i = 0; i < indexes.length; i++) {
      assertEquals(vector.get(indexes[i]), 5.0);
    }

    SparseDoubleLongKeyVector addVector3 = new SparseDoubleLongKeyVector(-1);
    for(int i = 0; i < indexes.length; i++) {
      addVector3.set(indexes[i], 1.0);
    }
    vector.plusBy(addVector3);
    for(int i = 0; i < indexes.length; i++) {
      assertEquals(vector.get(indexes[i]), 6.0);
    }

    CompSparseDoubleLongKeyVector clonedVector = (CompSparseDoubleLongKeyVector)vector.clone();
    for(int i = 0; i < indexes.length; i++) {
      assertEquals(clonedVector.get(indexes[i]), 6.0);
    }
  }

  private CompSparseDoubleLongKeyVector initVector(){
    PartitionKey [] partKeys = new PartitionKey[4];
    DoubleLongKeyVector [] vectors = new DoubleLongKeyVector[4];
    long blockCol = Long.MAX_VALUE / 2;
    partKeys[0] = new PartitionKey(0, 0, 0, Long.MIN_VALUE , 0, Long.MIN_VALUE + blockCol);
    partKeys[1] = new PartitionKey(0, 0, 0, Long.MIN_VALUE + blockCol , 0, Long.MIN_VALUE + blockCol * 2);
    partKeys[2] = new PartitionKey(0, 0, 0, Long.MIN_VALUE + blockCol * 2 , 0, Long.MIN_VALUE + blockCol * 3);
    partKeys[3] = new PartitionKey(0, 0, 0, Long.MIN_VALUE + blockCol * 3 , 0, Long.MAX_VALUE);

    vectors[0] = new SparseDoubleLongKeyVector(-1);
    vectors[1] = new SparseDoubleLongKeyVector(-1);
    vectors[2] = new SparseDoubleLongKeyVector(-1);
    vectors[3] = new SparseDoubleLongKeyVector(-1);

    return new CompSparseDoubleLongKeyVector(0, 0, -1, partKeys, vectors);
  }
}

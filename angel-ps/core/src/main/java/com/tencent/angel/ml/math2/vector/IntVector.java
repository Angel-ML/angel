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


package com.tencent.angel.ml.math2.vector;

import com.tencent.angel.ml.math2.storage.IntVectorStorage;
import it.unimi.dsi.fastutil.ints.IntIterator;

public abstract class IntVector extends Vector {
  public abstract int max();

  public abstract int min();

  @Override public double sum() {
    IntVectorStorage istorage = (IntVectorStorage) storage;
    double sumval = 0.0;
    if (istorage.isSparse()) {
      IntIterator iter = istorage.valueIterator();
      while (iter.hasNext()) {
        sumval += iter.nextInt();
      }
    } else {
      for (double val : istorage.getValues()) {
        sumval += val;
      }
    }
    return sumval;
  }

  @Override public double norm() {
    IntVectorStorage istorage = (IntVectorStorage) storage;
    double sumval2 = 0.0;
    if (istorage.isSparse()) {
      IntIterator iter = istorage.valueIterator();
      while (iter.hasNext()) {
        double val = iter.nextInt();
        sumval2 += val * val;
      }
    } else {
      for (double val : istorage.getValues()) {
        sumval2 += val * val;
      }
    }

    return Math.sqrt(sumval2);
  }
}

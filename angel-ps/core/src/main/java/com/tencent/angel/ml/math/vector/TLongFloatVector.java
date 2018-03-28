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
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package com.tencent.angel.ml.math.vector;

import com.tencent.angel.ml.math.TVector;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Base class for long key double vector.
 */
public abstract class TLongFloatVector extends TFloatVector {
  private static final Log LOG = LogFactory.getLog(SparseLongKeyFloatVector.class);

  /** Vector dimension */
  protected long dim;

  public TLongFloatVector(long dim) {
    super();
    this.dim = dim;
  }

  public long getLongDim(){
    return dim;
  }

  public abstract TVector plusBy(long index, float x);

  public abstract void set(long index, float x);

  public abstract float get(long index);

  public abstract long[] getIndexes();

  public abstract float[] getValues();

  @Override
  public abstract TLongFloatVector clone();

  @Override
  public int getDimension() {
    throw new UnsupportedOperationException("Unsupportted operation, you should use getLongDim instead");
  }

  public abstract double sum();

  public abstract TLongFloatVector elemUpdate(LongFloatElemUpdater updater, ElemUpdateParam param);
}

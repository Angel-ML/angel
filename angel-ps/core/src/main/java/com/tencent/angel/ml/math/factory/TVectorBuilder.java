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

package com.tencent.angel.ml.math.factory;

import com.tencent.angel.ml.math.TVector;
import com.tencent.angel.ml.math.vector.DenseDoubleVector;
import com.tencent.angel.ml.math.vector.DenseIntVector;
import com.tencent.angel.ml.math.vector.SparseDoubleVector;
import com.tencent.angel.ml.math.vector.SparseIntVector;

public interface TVectorBuilder {

  TVector build(int dimention);
}


class DenseDoubleVectorBuilder implements TVectorBuilder {

  public TVector build(int dimension) {
    return new DenseDoubleVector(dimension);
  }
}


class SparseDoubleVectorBuilder implements TVectorBuilder {
  public TVector build(int dimension) {
    return new SparseDoubleVector(dimension);
  }
}


class DenseIntVectorBuilder implements TVectorBuilder {
  public TVector build(int dimension) {
    return new DenseIntVector(dimension);
  }
}


class SparseIntVectorBuilder implements TVectorBuilder {
  public TVector build(int dimension) {
    return new SparseIntVector(dimension);
  }
}

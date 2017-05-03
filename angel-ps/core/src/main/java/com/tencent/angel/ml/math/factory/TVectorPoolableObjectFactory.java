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
import org.apache.commons.pool.PoolableObjectFactory;

public class TVectorPoolableObjectFactory<TVF extends TVectorBuilder> implements
    PoolableObjectFactory {

  private final int dimension;
  private final TVF factory;

  public TVectorPoolableObjectFactory(int dimension, TVF factory) {
    this.dimension = dimension;
    this.factory = factory;
  }

  @Override
  public Object makeObject() throws Exception {
    TVector vector = factory.build(dimension);
    return vector;
  }

  @Override
  public void destroyObject(Object obj) throws Exception {
    TVector vector = (TVector) obj;
    vector.clear();
  }

  @Override
  public boolean validateObject(Object obj) {
    return true;
  }

  @Override
  public void activateObject(Object obj) throws Exception {
    return;
  }

  @Override
  public void passivateObject(Object obj) throws Exception {
    TVector vector = (TVector) obj;
    vector.clear();
  }

}

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


package com.tencent.angel.ml.lda.algo.structures;

public abstract class TraverseHashMap {

  public TraverseHashMap(int expected) {
  }

  public abstract short get(final short k);

  public abstract short get(final int k);

  public abstract void put(final short k, final short v);

  public abstract void put(final int k, final int v);

  public abstract void rehash();

  public abstract short dec(final short k);

  public abstract short dec(final int k);

  public abstract short inc(final short k);

  public abstract short inc(final int k);

  public abstract int bytes();

  public abstract int size();

  public abstract short getKey(int idx);

  public abstract int getVal(int idx);

  public abstract int getIntKey(int idx);

  protected abstract int idx(int pos);

  protected abstract int poss(int pos);

  protected abstract boolean used(int pos);

}

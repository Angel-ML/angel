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


package com.tencent.angel.utils;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A simple atomic counter.
 */
public class PositiveAtomicCounter {
  private final AtomicInteger atom;
  private static final int mask = 0x7FFFFFFF;

  public PositiveAtomicCounter() {
    atom = new AtomicInteger(0);
  }

  public final int incrementAndGet() {
    final int rt = atom.incrementAndGet();
    return rt & mask;
  }

  public int intValue() {
    return atom.intValue();
  }
}
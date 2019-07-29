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


package com.tencent.angel.ps.storage.vector.storage;

/**
 * Inner storage method for the storage use a vector
 */
public enum StorageMethod {
  DENSE(0), SPARSE(1), SORTED(2);

  private final int value;

  StorageMethod(int value) {
    this.value = value;
  }

  public int getValue() {
    return value;
  }

  public static StorageMethod valuesOf(int value) {
    switch (value) {
      case 0:
        return DENSE;
      case 1:
        return SPARSE;
      case 2:
        return SORTED;
      default:
        return SPARSE;
    }
  }
}

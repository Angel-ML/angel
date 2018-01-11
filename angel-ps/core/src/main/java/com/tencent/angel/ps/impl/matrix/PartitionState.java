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

package com.tencent.angel.ps.impl.matrix;

/**
 * Partition state
 */
public enum PartitionState {
  INITIALIZING(0, PartitionState.INITIALIZE_VALUE),
  LOADING(1, PartitionState.LOADING_VALUE),
  RECOVERING(2, PartitionState.RECOVERING_VALUE),
  READ_ONLY(3, PartitionState.READ_ONLY_VALUE),
  READ_AND_WRITE(4, PartitionState.READ_AND_WRITE_VALUE),
  SAVING(5, PartitionState.SAVING_VALUE),
  INVALID(6, PartitionState.INVALID_VALUE);

  public final static int INITIALIZE_VALUE = 1;
  public final static int LOADING_VALUE = 2;
  public final static int RECOVERING_VALUE = 3;
  public final static int READ_ONLY_VALUE = 4;
  public final static int READ_AND_WRITE_VALUE = 5;
  public final static int SAVING_VALUE = 6;
  public final static int INVALID_VALUE = 7;

  public final int getNumber() {
    return value;
  }

  public static PartitionState valueOf(int value) {
    switch (value) {
      case INITIALIZE_VALUE:
        return INITIALIZING;
      case LOADING_VALUE:
        return LOADING;
      case RECOVERING_VALUE:
        return RECOVERING;
      case READ_ONLY_VALUE:
        return READ_ONLY;
      case READ_AND_WRITE_VALUE:
        return READ_AND_WRITE;
      case SAVING_VALUE:
        return SAVING;
      case INVALID_VALUE:
        return INVALID;
      default:
        return INVALID;
    }
  }

  private final int index;
  private final int value;

  private PartitionState(int index, int value) {
    this.index = index;
    this.value = value;
  }
}

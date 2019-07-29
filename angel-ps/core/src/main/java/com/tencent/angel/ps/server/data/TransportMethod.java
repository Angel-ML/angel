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


package com.tencent.angel.ps.server.data;

import java.util.HashMap;
import java.util.Map;

public enum TransportMethod {
  GET_ROWSPLIT(1), PUT_PARTUPDATE(2), GET_ROWSSPLIT(3), GET_PART(4), PUT_PART(5), GET_CLOCKS(
      6), UPDATE_PSF(7), GET_PSF(8), RECOVER_PART(9), UPDATE_CLOCK(10), UPDATE(11), INDEX_GET_ROW(
      12), INDEX_GET_ROWS(13), CHECKPOINT(14), GET_STATE(15), UNKNOWN(16);

  public static Map<Integer, TransportMethod> typeIdToTypeMap;

  static {
    typeIdToTypeMap = new HashMap<>();
    typeIdToTypeMap.put(GET_ROWSPLIT.methodId, GET_ROWSPLIT);
    typeIdToTypeMap.put(PUT_PARTUPDATE.methodId, PUT_PARTUPDATE);
    typeIdToTypeMap.put(GET_ROWSSPLIT.methodId, GET_ROWSSPLIT);
    typeIdToTypeMap.put(GET_PART.methodId, GET_PART);
    typeIdToTypeMap.put(PUT_PART.methodId, PUT_PART);
    typeIdToTypeMap.put(GET_CLOCKS.methodId, GET_CLOCKS);
    typeIdToTypeMap.put(UPDATE_PSF.methodId, UPDATE_PSF);
    typeIdToTypeMap.put(GET_PSF.methodId, GET_PSF);
    typeIdToTypeMap.put(RECOVER_PART.methodId, RECOVER_PART);
    typeIdToTypeMap.put(UPDATE_CLOCK.methodId, UPDATE_CLOCK);
    typeIdToTypeMap.put(UPDATE.methodId, UPDATE);
    typeIdToTypeMap.put(INDEX_GET_ROW.methodId, INDEX_GET_ROW);
    typeIdToTypeMap.put(INDEX_GET_ROWS.methodId, INDEX_GET_ROWS);
    typeIdToTypeMap.put(CHECKPOINT.methodId, CHECKPOINT);
    typeIdToTypeMap.put(GET_STATE.methodId, GET_STATE);
    typeIdToTypeMap.put(UNKNOWN.methodId, UNKNOWN);
  }

  public static TransportMethod valueOf(int id) {
    return typeIdToTypeMap.get(id);
  }

  private final int methodId;

  TransportMethod(int methodId) {
    this.methodId = methodId;
  }

  public int getMethodId() {
    return methodId;
  }
}

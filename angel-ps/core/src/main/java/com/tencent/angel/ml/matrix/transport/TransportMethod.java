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
 */

package com.tencent.angel.ml.matrix.transport;

import java.util.HashMap;
import java.util.Map;

public enum TransportMethod {
  GET_ROWSPLIT(1), PUT_PARTUPDATE(2), GET_ROWSSPLIT(3), GET_PART(4), PUT_PART(5), GET_CLOCKS(6), UPDATER(
      7), GET_UDF(8), RECOVER_PART(9), UPDATE_CLOCK(10);

  public static Map<Integer, TransportMethod> typeIdToTypeMap;
  static {
    typeIdToTypeMap = new HashMap<Integer, TransportMethod>();
    typeIdToTypeMap.put(GET_ROWSPLIT.methodId, GET_ROWSPLIT);
    typeIdToTypeMap.put(PUT_PARTUPDATE.methodId, PUT_PARTUPDATE);
    typeIdToTypeMap.put(GET_ROWSSPLIT.methodId, GET_ROWSSPLIT);
    typeIdToTypeMap.put(GET_PART.methodId, GET_PART);
    typeIdToTypeMap.put(PUT_PART.methodId, PUT_PART);
    typeIdToTypeMap.put(GET_CLOCKS.methodId, GET_CLOCKS);
    typeIdToTypeMap.put(UPDATER.methodId, UPDATER);
    typeIdToTypeMap.put(GET_UDF.methodId, GET_UDF);
    typeIdToTypeMap.put(RECOVER_PART.methodId, RECOVER_PART);
    typeIdToTypeMap.put(UPDATE_CLOCK.methodId, UPDATE_CLOCK);
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

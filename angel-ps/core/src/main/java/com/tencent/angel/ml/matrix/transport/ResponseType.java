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

public enum ResponseType {
  SUCCESS(1),
  SERVER_NOT_READY(2),
  CONNECT_REFUSED(3),
  NETWORK_ERROR(4),
  TIMEOUT(5),
  SERVER_HANDLE_FAILED(6),
  SERVER_HANDLE_FATAL(7),
  CLOCK_NOTREADY(8),
  PARTITION_READ_ONLY(9),
  SERVER_IS_BUSY(10),
  UNKNOWN_ERROR(11),
  OOM(12);

  public static Map<Integer, ResponseType> typeIdToTypeMap;
  static {
    typeIdToTypeMap = new HashMap<Integer, ResponseType>();
    typeIdToTypeMap.put(SUCCESS.typeId, SUCCESS);
    typeIdToTypeMap.put(SERVER_NOT_READY.typeId, SERVER_NOT_READY);
    typeIdToTypeMap.put(CONNECT_REFUSED.typeId, CONNECT_REFUSED);
    typeIdToTypeMap.put(NETWORK_ERROR.typeId, NETWORK_ERROR);
    typeIdToTypeMap.put(TIMEOUT.typeId, TIMEOUT);
    typeIdToTypeMap.put(SERVER_HANDLE_FAILED.typeId, SERVER_HANDLE_FAILED);
    typeIdToTypeMap.put(SERVER_HANDLE_FATAL.typeId, SERVER_HANDLE_FATAL);
    typeIdToTypeMap.put(CLOCK_NOTREADY.typeId, CLOCK_NOTREADY);
    typeIdToTypeMap.put(PARTITION_READ_ONLY.typeId, PARTITION_READ_ONLY);
    typeIdToTypeMap.put(SERVER_IS_BUSY.typeId, SERVER_IS_BUSY);
    typeIdToTypeMap.put(UNKNOWN_ERROR.typeId, UNKNOWN_ERROR);
    typeIdToTypeMap.put(OOM.typeId, OOM);
  }


  public static ResponseType valueOf(int id) {
    return typeIdToTypeMap.get(id);
  }

  private final int typeId;

  ResponseType(int methodId) {
    this.typeId = methodId;
  }

  public int getTypeId() {
    return typeId;
  }
}

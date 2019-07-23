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


package com.tencent.angel.ps.server.data.request;

import java.util.HashMap;
import java.util.Map;

public enum IndexType {
  INT(1), LONG(2);

  public static Map<Integer, IndexType> typeIdToTypeMap;

  static {
    typeIdToTypeMap = new HashMap<>();
    typeIdToTypeMap.put(INT.typeId, INT);
    typeIdToTypeMap.put(LONG.typeId, LONG);
  }

  public static IndexType valueOf(int id) {
    return typeIdToTypeMap.get(id);
  }

  private final int typeId;

  IndexType(int typeId) {
    this.typeId = typeId;
  }

  public int getTypeId() {
    return typeId;
  }
}

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


package com.tencent.angel.model;

import java.util.HashMap;
import java.util.Map;

public enum SaveState {
  INIT(1), SAVING(2), COMBINING(3), SUCCESS(4), FAILED(5);

  public static Map<Integer, SaveState> stateIdToStateMap;

  static {
    stateIdToStateMap = new HashMap<>();
    stateIdToStateMap.put(INIT.stateId, INIT);
    stateIdToStateMap.put(SAVING.stateId, SAVING);
    stateIdToStateMap.put(COMBINING.stateId, COMBINING);
    stateIdToStateMap.put(SUCCESS.stateId, SUCCESS);
    stateIdToStateMap.put(FAILED.stateId, FAILED);
  }

  public static SaveState valueOf(int id) {
    return stateIdToStateMap.get(id);
  }

  private final int stateId;

  SaveState(int stateId) {
    this.stateId = stateId;
  }

  public int getStateId() {
    return stateId;
  }
}
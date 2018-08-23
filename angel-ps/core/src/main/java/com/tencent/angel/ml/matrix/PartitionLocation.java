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


package com.tencent.angel.ml.matrix;

import com.tencent.angel.ps.server.data.PSLocation;

import java.util.List;

/**
 * The stored ps ids and locations for a partition
 */
public class PartitionLocation {
  public final List<PSLocation> psLocs;

  /**
   * Create a PartitionLocation
   *
   * @param psLocs ps location
   */
  public PartitionLocation(List<PSLocation> psLocs) {
    this.psLocs = psLocs;
  }

  @Override public String toString() {
    StringBuilder sb = new StringBuilder();
    int size = psLocs.size();
    sb.append("partition location:{");
    for (int i = 0; i < size; i++) {
      sb.append("server:").append(psLocs.get(i));
    }
    sb.append("}");
    return sb.toString();
  }
}

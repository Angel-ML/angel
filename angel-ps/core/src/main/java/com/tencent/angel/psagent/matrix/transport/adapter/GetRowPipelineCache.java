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


package com.tencent.angel.psagent.matrix.transport.adapter;

import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ps.storage.vector.ServerRow;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The result cache for GET_ROW sub-requests.
 */
public class GetRowPipelineCache extends PartitionResponseCache<ServerRow> {
  private static final Log LOG = LogFactory.getLog(GetRowPipelineCache.class);

  /**
   * merge startup ratio
   */
  private final double startMergeRatio;

  /**
   * row type
   */
  private final RowType rowType;

  public final AtomicBoolean merging = new AtomicBoolean(false);

  /**
   * Create a new GetRowPipelineCache.
   *
   * @param totalRequestNum the number of sub-requests
   * @param rowType         row type
   */
  public GetRowPipelineCache(int totalRequestNum, RowType rowType) {
    super(totalRequestNum);
    this.rowType = rowType;
    startMergeRatio = 0.3;
  }

  @Override public boolean canMerge() {
    // Now we just support pipelined row splits merging for dense type row
    if (rowType == RowType.T_DOUBLE_DENSE || rowType == RowType.T_INT_DENSE
      || rowType == RowType.T_FLOAT_DENSE) {
      return getProgress() >= startMergeRatio;
    } else {
      return super.canMerge();
    }
  }

  /**
   * Get row type
   *
   * @return RowType row type
   */
  public RowType getRowType() {
    return rowType;
  }

  @Override public String toString() {
    return "GetRowPipelineCache{" + "startMergeRatio=" + startMergeRatio + ", rowType=" + rowType
      + "} " + super.toString();
  }
}

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


package com.tencent.angel.ml.matrix.psf.update.update;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.oplog.cache.RowUpdateSplit;
import com.tencent.angel.psagent.matrix.oplog.cache.RowUpdateSplitContext;
import com.tencent.angel.psagent.matrix.oplog.cache.RowUpdateSplitUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Increment rows parameters
 */
public class IncrementRowsParam extends UpdateParam {

  protected Vector[] updates;

  /**
   * Update vectors
   */

  /**
   * Create increment rows params
   *
   * @param matrixId matrix id
   * @param updates increment rows
   */
  public IncrementRowsParam(int matrixId, Vector[] updates) {
    super(matrixId);
    this.updates = updates;
  }

  @Override
  public List<PartitionUpdateParam> split() {
    // Split updates
    Map<PartitionKey, List<RowUpdateSplit>> partToSplits = new HashMap<>(getPartsNum(matrixId));
    for (int i = 0; i < updates.length; i++) {
      if (updates[i] != null) {
        mergeRowUpdateSplits(RowUpdateSplitUtils
            .split(updates[i], getParts(matrixId, updates[i].getRowId())), partToSplits);
      }
    }

    // Shuffle update splits
    shuffleSplits(partToSplits);

    // Generate part update parameters
    List<PartitionUpdateParam> partParams = new ArrayList<>(partToSplits.size());
    for (Entry<PartitionKey, List<RowUpdateSplit>> partEntry : partToSplits.entrySet()) {
      // Set split context: partition key, use int key for long key vector or not ect
      adapt(partEntry.getKey(), partEntry.getValue());

      partParams
          .add(new PartIncrementRowsParam(matrixId, partEntry.getKey(), partEntry.getValue()));
    }
    return partParams;
  }

  protected void shuffleSplits(Map<PartitionKey, List<RowUpdateSplit>> partToSplits) {
    for (List<RowUpdateSplit> splits : partToSplits.values()) {
      shuffleSplits(splits);
    }
  }

  protected void adapt(PartitionKey part, List<RowUpdateSplit> splits) {
    RowUpdateSplitContext context = new RowUpdateSplitContext();
    context.setPartKey(part);
    for (RowUpdateSplit split : splits) {
      split.setSplitContext(context);
      split.setUseIntKey(useIntKey(part));
    }
  }

  private boolean useIntKey(PartitionKey part) {
    boolean useAdaptor = PSAgentContext.get().getConf()
        .getBoolean(AngelConf.ANGEL_PS_USE_ADAPTIVE_STORAGE_ENABLE,
            AngelConf.DEFAULT_ANGEL_PS_USE_ADAPTIVE_STORAGE_ENABLE);
    return useAdaptor && (part.getEndCol() - part.getStartCol() < Integer.MAX_VALUE);
  }

  private void shuffleSplits(List<RowUpdateSplit> splits) {
    Collections.shuffle(splits);
  }

  protected int getPartsNum(int matrixId) {
    return getParts(matrixId).size();
  }

  private int getPartsNum(int matrixId, int rowId) {
    return getParts(matrixId, rowId).size();
  }

  protected List<PartitionKey> getParts(int matrixId) {
    return PSAgentContext.get().getPsAgent().getMatrixMetaManager()
        .getPartitions(matrixId);
  }

  protected List<PartitionKey> getParts(int matrixId, int rowId) {
    return PSAgentContext.get().getPsAgent().getMatrixMetaManager()
        .getPartitions(matrixId, rowId);
  }

  protected void mergeRowUpdateSplits(Map<PartitionKey, RowUpdateSplit> rowSplits,
      Map<PartitionKey, List<RowUpdateSplit>> partToSplits) {
    for (Entry<PartitionKey, RowUpdateSplit> entry : rowSplits.entrySet()) {
      List<RowUpdateSplit> splits = partToSplits.get(entry.getKey());
      if (splits == null) {
        splits = new ArrayList<>(updates.length);
        partToSplits.put(entry.getKey(), splits);
      }
      splits.add(entry.getValue());
    }
  }
}

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

package com.tencent.angel.graph.client.psf.init.initnodefeats;

import com.tencent.angel.graph.client.psf.init.GeneralInitParam;
import com.tencent.angel.graph.data.Feature;
import com.tencent.angel.graph.data.FeatureKeyType;
import com.tencent.angel.graph.data.GraphNode;
import com.tencent.angel.graph.utils.GraphMatrixUtils;
import com.tencent.angel.ml.matrix.psf.update.base.GeneralPartUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.ps.storage.vector.element.ByteArrayElement;
import com.tencent.angel.ps.storage.vector.element.IElement;
import com.tencent.angel.psagent.matrix.transport.router.operator.ILongKeyAnyValuePartOp;

/**
 * Init node feature for GraphNode
 */
public class InitNodeFeats extends UpdateFunc {

  /**
   * Create a new UpdateParam
   */
  public InitNodeFeats(InitNodeFeatureParam param) {
    super(param);
  }

  public InitNodeFeats() {
    this(null);
  }

  @Override
  public void partitionUpdate(PartitionUpdateParam partParam) {
    PartInitNodeFeatureParam initParam = (PartInitNodeFeatureParam) partParam;
    ServerLongAnyRow row = GraphMatrixUtils.getPSLongKeyRow(psContext, initParam);
    FeatureKeyType keyType = initParam.getKeyType();

    // Get node ids and features
    ILongKeyAnyValuePartOp split = (ILongKeyAnyValuePartOp) initParam.getKeyValuePart();
    long[] nodeIds = split.getKeys();
    IElement[] features = split.getValues();

    row.startWrite();
    try {
      if(nodeIds.length != features.length) {
        throw new IllegalArgumentException("nodeIds length does not match features length");
      }
      for (int i = 0; i < nodeIds.length; i++) {
        GraphNode graphNode = (GraphNode) row.get(nodeIds[i]);
        if (graphNode == null) {
          graphNode = new GraphNode();
          row.set(nodeIds[i], graphNode);
        }
        graphNode.setFeats((ByteArrayElement) features[i], keyType);
      }
    } finally {
      row.endWrite();
    }
  }
}
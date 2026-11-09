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

package com.tencent.angel.graph.model.neighbor.dynamicsimple.psf;

import com.tencent.angel.graph.common.psf.param.LongKeysUpdateParam;
import com.tencent.angel.graph.model.neighbor.dynamicsimple.DynamicSimpleNeighborElement;
import com.tencent.angel.graph.utils.GraphMatrixUtils;
import com.tencent.angel.ml.matrix.psf.update.base.GeneralPartUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.ps.storage.vector.element.IElement;
import com.tencent.angel.ps.storage.vector.element.IntFloatArrayPairElement;
import com.tencent.angel.psagent.matrix.transport.router.operator.ILongKeyAnyValuePartOp;


public class InitAlias extends UpdateFunc {

  public InitAlias(LongKeysUpdateParam param) {
    super(param);
  }

  public InitAlias() {
    this(null);
  }

  @Override
  public void partitionUpdate(PartitionUpdateParam partParam) {
    GeneralPartUpdateParam initParam = (GeneralPartUpdateParam) partParam;
    ServerLongAnyRow row = GraphMatrixUtils.getPSLongKeyRow(psContext, initParam);
    ILongKeyAnyValuePartOp split = (ILongKeyAnyValuePartOp) initParam.getKeyValuePart();
    long[] nodeIds = split.getKeys();
    IElement[] alias = split.getValues();

    row.startWrite();
    try {
      for(int i = 0; i < nodeIds.length; i++) {
        DynamicSimpleNeighborElement ele = (DynamicSimpleNeighborElement) row.get(nodeIds[i]);
        IntFloatArrayPairElement data = (IntFloatArrayPairElement) alias[i];
        ele.setAlias(data.getData());
        ele.setWeights(data.getWeights());
      }
    } finally {
      row.endWrite();
    }
  }
}

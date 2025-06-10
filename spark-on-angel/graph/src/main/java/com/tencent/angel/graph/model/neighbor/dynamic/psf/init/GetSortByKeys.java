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

package com.tencent.angel.graph.model.neighbor.dynamic.psf.init;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.graph.model.neighbor.dynamic.DynamicNeighborElement;
import com.tencent.angel.graph.utils.GraphMatrixUtils;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.transport.router.KeyPart;
import com.tencent.angel.psagent.matrix.transport.router.RouterUtils;
import com.tencent.angel.psagent.matrix.transport.router.operator.ILongKeyPartOp;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

/**
 * Init node neighbors for long type node id
 */
public class GetSortByKeys extends UpdateFunc {

  /**
   * Create a new UpdateParam
   */
  public GetSortByKeys(UpdateParam param) { super(param); }

  public GetSortByKeys() { this(null); }

  public static class GetSortByKeysParam extends UpdateParam {

    private final long[] nodeIds;

    /**
     * Instantiates a new Zero updater param.
     *
     * @param matrixId    the matrix id
     * @param nodeIds
     */
    public GetSortByKeysParam(int matrixId, long[] nodeIds) {
      super(matrixId);
      this.nodeIds = nodeIds;
    }

    public GetSortByKeysParam() { this(-1, null);}

    public long[] getNodeIds() {
      return nodeIds;
    }

    @Override public List<PartitionUpdateParam> split() {
      MatrixMeta meta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
      PartitionKey[] parts = meta.getPartitionKeys();

      KeyPart[] nodeIdsParts = RouterUtils.split(meta, 0, nodeIds, false);

      List<PartitionUpdateParam> partParams = new ArrayList<>(parts.length);
      assert parts.length == nodeIdsParts.length;
      for (int i = 0; i < parts.length; i++) {
        if (nodeIdsParts[i] != null && nodeIdsParts[i].size() > 0) {
          partParams.add(new PartGetSorByKeysParam(matrixId, parts[i], nodeIdsParts[i]));
        }
      }
      return partParams;
    }
  }

  public static class PartGetSorByKeysParam extends PartitionUpdateParam {

    /**
     * Key and value data partition
     */
    private KeyPart keyPart;

    public PartGetSorByKeysParam(int matrixId, PartitionKey partKey, KeyPart keyPart) {
      super(matrixId, partKey);
      this.keyPart = keyPart;
    }

    public PartGetSorByKeysParam() {
      this(-1, null, null);
    }

    @Override
    public void serialize(ByteBuf buf) {
      super.serialize(buf);
      ByteBufSerdeUtils.serializeKeyPart(buf, keyPart);
    }

    @Override
    public void deserialize(ByteBuf buf) {
      super.deserialize(buf);
      keyPart = ByteBufSerdeUtils.deserializeKeyPart(buf);
    }

    @Override
    public int bufferLen() {
      return super.bufferLen() + ByteBufSerdeUtils.serializedKeyPartLen(keyPart);
    }

    public KeyPart getKeyPart() {
      return keyPart;
    }

    public void setKeyPart(KeyPart keyPart) {
      this.keyPart = keyPart;
    }
  }

  @Override
  public void partitionUpdate(PartitionUpdateParam partParam) {
    PartGetSorByKeysParam param = (PartGetSorByKeysParam) partParam;
    KeyPart split = param.getKeyPart();
    long[] nodes = ((ILongKeyPartOp) split).getKeys();
    ServerLongAnyRow row = GraphMatrixUtils.getPSLongKeyRow(psContext, param);
    row.startWrite();
    try {
      for (long node: nodes) {
        DynamicNeighborElement ele = (DynamicNeighborElement) row.get(node);
        if (ele != null) {
          ele.trans();
        }
      }
    } finally {
      row.endWrite();
    }
  }
}

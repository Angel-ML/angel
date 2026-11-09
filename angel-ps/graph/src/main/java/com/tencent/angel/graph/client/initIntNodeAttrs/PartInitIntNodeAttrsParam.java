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

package com.tencent.angel.graph.client.initIntNodeAttrs;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;

public class PartInitIntNodeAttrsParam extends PartitionUpdateParam {

  /**
   * Int Node id to Attrs
   */
  private Int2ObjectMap<float[]> nodeIdToAttrIndices;

  private int[] nodeIds;
  private transient int startIndex;
  private transient int endIndex;

  public PartInitIntNodeAttrsParam(int matrixId, PartitionKey partKey,
      Int2ObjectMap<float[]> nodeIdToAttrIndices, int[] nodeIds, int startIndex,
      int endIndex) {
    super(matrixId, partKey);
    this.nodeIdToAttrIndices = nodeIdToAttrIndices;
    this.nodeIds = nodeIds;
    this.startIndex = startIndex;
    this.endIndex = endIndex;
  }

  public PartInitIntNodeAttrsParam() {
    this(0, null, null, null, 0, 0);
  }

  public Int2ObjectMap<float[]> getNodeIdToAttrIndices() {
    return nodeIdToAttrIndices;
  }

  private void clear() {
    nodeIdToAttrIndices = null;
    nodeIds = null;
    startIndex = -1;
    endIndex = -1;
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    int nodeId;
    float[] attrs;
    int writeIndex = buf.writerIndex();
    int writeNum = 0;
    buf.writeInt(0);
    for (int i = startIndex; i < endIndex; i++) {
      nodeId = nodeIds[i];
      attrs = nodeIdToAttrIndices.get(nodeId);
      if (attrs == null || attrs.length == 0) {
        continue;
      }
      buf.writeInt(nodeId);
      buf.writeInt(attrs.length);
      for (float attr : attrs) {
        buf.writeFloat(attr);
      }
      writeNum++;
    }
    buf.setInt(writeIndex, writeNum);

    clear();
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    int len = buf.readInt();
    nodeIdToAttrIndices = new Int2ObjectArrayMap<>(len);

    for (int i = 0; i < len; i++) {
      int nodeId = buf.readInt();
      int attrNum = buf.readInt();
      float[] attr = new float[attrNum];
      for (int j = 0; j < attrNum; j++) {
        attr[j] = buf.readFloat();
      }
      nodeIdToAttrIndices.put(nodeId, attr);
    }
  }

  @Override
  public int bufferLen() {
    int len = super.bufferLen();
    len += 4;
    for (int i = startIndex; i < endIndex; i++) {
      if (nodeIdToAttrIndices.get(nodeIds[i]) != null) {
        len += 4 * nodeIdToAttrIndices.get(nodeIds[i]).length;
      }
      len += 8;
    }
    return len;
  }
}

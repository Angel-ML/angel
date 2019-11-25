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
package com.tencent.angel.spark.ml.psf.clusterrank;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;

public class PartInitOutDegreeParam extends PartitionUpdateParam {

    private Long2IntMap nodeIdToOutDegree;

    private long[] nodeIds;
    private transient int startIndex;
    private transient int endIndex;

    public PartInitOutDegreeParam(int matrixId, PartitionKey partKey,
            Long2IntMap nodeIdToOutDegree, long[] nodeIds, int startIndex, int endIndex) {
        super(matrixId, partKey);
        this.nodeIdToOutDegree = nodeIdToOutDegree;
        this.nodeIds = nodeIds;
        this.startIndex = startIndex;
        this.endIndex = endIndex;
    }

    public PartInitOutDegreeParam() {
        this(0, null, null, null, 0, 0);
    }

    public Long2IntMap getNodeIdToOutDegree() {
        return nodeIdToOutDegree;
    }

    public long[] getNodeIds() {
        return nodeIds;
    }

    private void clear() {
        nodeIdToOutDegree = null;
        nodeIds = null;
        startIndex = -1;
        endIndex = -1;
    }

    @Override
    public void serialize(ByteBuf buf) {
        super.serialize(buf);
        long nodeId;
        int degree;
        int writeIndex = buf.writerIndex();
        int writeNum = 0;
        buf.writeInt(0);
        for (int i = startIndex; i < endIndex; i++) {
            nodeId = nodeIds[i];
            degree = nodeIdToOutDegree.get(nodeId);
            buf.writeLong(nodeId);
            buf.writeInt(degree);
            writeNum++;
        }
        buf.setInt(writeIndex, writeNum);

        clear();
    }

    @Override
    public void deserialize(ByteBuf buf) {
        super.deserialize(buf);
        int len = buf.readInt();
        nodeIdToOutDegree = new Long2IntOpenHashMap(len);

        for (int i = 0; i < len; i++) {
            long nodeId = buf.readLong();
            int degree = buf.readInt();
            nodeIdToOutDegree.put(nodeId, degree);
        }
    }

    @Override
    public int bufferLen() {
        int len = super.bufferLen();
        len += 4;
        int numItem = nodeIdToOutDegree.size();
        len += 8 * numItem; // key
        len += 4 * numItem; // value
        return len;
    }


}

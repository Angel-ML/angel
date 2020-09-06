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

package com.tencent.angel.graph.psf.neighbors.samplebyaliastable.samplealiastable;

import com.tencent.angel.ps.storage.vector.element.IElement;
import io.netty.buffer.ByteBuf;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * ps ：node's data (neighbors, accept, alias)
 */
public class NeighborsAliasTableElement implements IElement {

    private long[] neighborIds;
    private float[] accept;
    private int[] alias;


    public NeighborsAliasTableElement() {
        this(null, null, null);
    }

    public NeighborsAliasTableElement(long[] neighborIds, float[] accept, int[] alias) {
        assert (neighborIds.length == accept.length && neighborIds.length == alias.length );
        this.neighborIds = neighborIds;
        this.alias = alias;
        this.accept = accept;
    }

    public long[] getNeighborIds() {
        return neighborIds;
    }

    public float[] getAccept() {
        return accept;
    }

    public int[] getAlias() {
        return alias;
    }

    public int getNodesNum() {
        return neighborIds.length;
    }

    @Override
    public Object deepClone() {
        int len = neighborIds.length;
        long[] newNodeIds = new long[len];
        int[] newAlias = new int[len];
        float[] newAccept = new float[len];

        System.arraycopy(neighborIds, 0, newNodeIds, 0, len);
        System.arraycopy(accept, 0, newAccept, 0, len);
        System.arraycopy(alias, 0, newAlias, 0, len);
        return new NeighborsAliasTableElement(newNodeIds, newAccept, newAlias);
    }

    @Override
    public void serialize(ByteBuf output) {
        int len = neighborIds.length;
        output.writeInt(len);
        for (int i = 0; i < len; i++) {
            output.writeLong(neighborIds[i]);
        }
        for (int i = 0; i < len; i++) {
            output.writeFloat(accept[i]);
        }
        for (int i = 0; i < len; i++) {
            output.writeInt(alias[i]);
        }
    }

    @Override
    public void deserialize(ByteBuf input) {
        int len = input.readInt();
        neighborIds = new long[len];
        alias = new int[len];
        accept = new float[len];

        for (int i = 0; i < len; i++) {
            neighborIds[i] = input.readLong();
        }

        for (int i = 0; i < len; i++) {
            accept[i] = input.readFloat();
        }

        for (int i = 0; i < len; i++) {
            alias[i] = input.readInt();
        }
    }

    @Override
    public void serialize(DataOutputStream output) throws IOException {
        int len = neighborIds.length;
        output.writeInt(len);
        for (int i = 0; i < len; i++) {
            output.writeLong(neighborIds[i]);
        }
        for (int i = 0; i < len; i++) {
            output.writeFloat(accept[i]);
        }
        for (int i = 0; i < len; i++) {
            output.writeInt(alias[i]);
        }
    }

    @Override
    public void deserialize(DataInputStream input) throws IOException {
        int len = input.readInt();
        neighborIds = new long[len];
        accept = new float[len];
        alias = new int[len];

        for (int i = 0; i < len; i++) {
            neighborIds[i] = input.readLong();
        }

        for (int i = 0; i < len; i++) {
            accept[i] = input.readFloat();
        }

        for (int i = 0; i < len; i++) {
            alias[i] = input.readInt();
        }
    }

    @Override
    public int bufferLen() {
        return Integer.BYTES + neighborIds.length * Long.BYTES + accept.length * Float.BYTES + alias.length * Integer.BYTES;
    }

    @Override
    public int dataLen() {
        return bufferLen();
    }

}

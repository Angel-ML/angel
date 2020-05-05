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
package com.tencent.angel.graph.psf.triangle;

import com.tencent.angel.ps.storage.vector.element.IElement;
import io.netty.buffer.ByteBuf;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class NeighborsAttrsElement implements IElement {

    private long[] neighborIds;
    private byte[] attrs;

    public NeighborsAttrsElement() {
        this(null, null);
    }

    public NeighborsAttrsElement(long[] neighborIds, byte[] attrs) {
        assert (neighborIds.length == attrs.length);
        this.neighborIds = neighborIds;
        this.attrs = attrs;
    }

    public long[] getNeighborIds() {
        return neighborIds;
    }

    public byte[] getAttrs() {
        return attrs;
    }

    public int getNumNodes() {
        return neighborIds.length;
    }

    @Override
    public Object deepClone() {
        int len = neighborIds.length;
        long[] nodeIds = new long[len];
        byte[] newAttrs = new byte[len];
        System.arraycopy(neighborIds, 0, nodeIds, 0, len);
        System.arraycopy(attrs, 0, newAttrs, 0, len);
        return new NeighborsAttrsElement(nodeIds, newAttrs);
    }

    @Override
    public void serialize(ByteBuf output) {
        int len = neighborIds.length;
        output.writeInt(len);
        for (int i = 0; i < len; i++) {
            output.writeLong(neighborIds[i]);
        }
        for (int i = 0; i < len; i++) {
            output.writeByte(attrs[i]);
        }

    }

    @Override
    public void deserialize(ByteBuf input) {
        int len = input.readInt();
        neighborIds = new long[len];
        attrs = new byte[len];

        for (int i = 0; i < len; i++) {
            neighborIds[i] = input.readLong();
        }
        for (int i = 0; i < len; i++) {
            attrs[i] = input.readByte();
        }
    }

    @Override
    public int bufferLen() {
        return Integer.BYTES + neighborIds.length * Long.BYTES + attrs.length * Byte.BYTES;
    }

    @Override
    public void serialize(DataOutputStream output) throws IOException {
        int len = neighborIds.length;
        output.writeInt(len);
        for (int i = 0; i < len; i++) {
            output.writeLong(neighborIds[i]);
        }
        for (int i = 0; i < len; i++) {
            output.writeByte(attrs[i]);
        }
    }

    @Override
    public void deserialize(DataInputStream input) throws IOException {
        int len = input.readInt();
        neighborIds = new long[len];
        attrs = new byte[len];

        for (int i = 0; i < len; i++) {
            neighborIds[i] = input.readLong();
        }
        for (int i = 0; i < len; i++) {
            attrs[i] = input.readByte();
        }
    }

    @Override
    public int dataLen() {
        return bufferLen();
    }
}

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


package com.tencent.angel.ps.storage.vector.element;

import io.netty.buffer.ByteBuf;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * The complex object that contains long ele, it can be stored in PS
 */
public class LongElement implements IElement {

    private long data;

    public LongElement(long data) {
        this.data = data;
    }

    public LongElement() {
        this(-1);
    }

    public long getData() {
        return data;
    }

    public void setData(long data) {
        this.data = data;
    }

    @Override
    public LongElement deepClone() {
        return new LongElement(data);
    }

    @Override
    public void serialize(ByteBuf output) {
        output.writeLong(data);
    }

    @Override
    public void deserialize(ByteBuf input) {
        data = input.readLong();
    }

    @Override
    public int bufferLen() {
        return 8;
    }

    @Override
    public void serialize(DataOutputStream output) throws IOException {
        output.writeLong(data);
    }

    @Override
    public void deserialize(DataInputStream input) throws IOException {
        data = input.readLong();
    }

    @Override
    public int dataLen() {
        return bufferLen();
    }
}

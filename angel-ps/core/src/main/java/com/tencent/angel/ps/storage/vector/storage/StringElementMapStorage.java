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


package com.tencent.angel.ps.storage.vector.storage;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.common.Serialize;
import com.tencent.angel.ps.storage.vector.element.IElement;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map.Entry;

public class StringElementMapStorage extends StringElementStorage {

    private Object2ObjectOpenHashMap<String, IElement> data;
    private volatile String[] keys;

    public StringElementMapStorage(
            Class<? extends IElement> objectClass, int len, long indexOffset) {
        this(objectClass, new Object2ObjectOpenHashMap<String, IElement>(len), indexOffset);
    }

    public StringElementMapStorage(
            Class<? extends IElement> objectClass, Object2ObjectOpenHashMap<String, IElement> data, long indexOffset) {
        super(objectClass, indexOffset);
        this.data = data;
    }

    public StringElementMapStorage() {
        this(null, 0, 0L);
    }

    @Override
    public IElement get(String index) {
        return data.get(index);
    }

    @Override
    public void set(String index, IElement value) {
        data.put(index, value);
    }

    @Override
    public ObjectIterator<Object2ObjectMap.Entry<String, IElement>> iterator() {
        return this.data.object2ObjectEntrySet().fastIterator();
    }

    public String getKey(int index) {
        if (keys == null) {
            synchronized (StringElementMapStorage.class) {
                if (keys == null) {
                    keys = data.keySet().toArray(new String[0]);
                }
            }
        }
        return keys[index];
    }

    @Override
    public IElement[] get(String[] indices) {
        IElement[] result = new IElement[indices.length];
        for (int i = 0; i < indices.length; i++) {
            result[i] = get(indices[i]);
        }
        return result;
    }

    @Override
    public void set(String[] indices, IElement[] values) {
        assert indices.length == values.length;
        for (int i = 0; i < indices.length; i++) {
            set(indices[i], values[i]);
        }
    }

    @Override
    public boolean exist(String index) {
        return data.containsKey(index);
    }

    @Override
    public void clear() {
        data.clear();
    }

    @Override
    public StringElementMapStorage deepClone() {
        Object2ObjectOpenHashMap<String, IElement> clonedData = new Object2ObjectOpenHashMap(data.size());
        ObjectIterator<Object2ObjectMap.Entry<String, IElement>> iter = clonedData
                .object2ObjectEntrySet().fastIterator();
        while(iter.hasNext()) {
            Object2ObjectMap.Entry<String, IElement> entry = iter.next();
            clonedData.put(entry.getKey(), (IElement) entry.getValue().deepClone());
        }
        return new StringElementMapStorage(objectClass, clonedData, indexOffset);
    }

    @Override
    public int size() {
        return data.size();
    }

    @Override
    public boolean isDense() {
        return false;
    }

    @Override
    public boolean isSparse() {
        return true;
    }

    @Override
    public boolean isSorted() {
        return false;
    }

    @Override
    public StringElementMapStorage adaptiveClone() {
        return this;
    }

    @Override
    public void serialize(ByteBuf buf) {
        super.serialize(buf);
        // Valid element number
        int writeIndex = buf.writerIndex();
        buf.writeInt(0);

        // Element data
        int writeNum = 0;
        for (Entry<String, IElement> entry : data.entrySet()) {
            ByteBufSerdeUtils.serializeUTF8(buf, entry.getKey());
            ByteBufSerdeUtils.serializeObject(buf, entry.getValue());
            writeNum++;
        }

        buf.setInt(writeIndex, writeNum);
    }

    @Override
    public void deserialize(ByteBuf buf) {
        super.deserialize(buf);

        // Valid element number
        int elementNum = buf.readInt();
        data = new Object2ObjectOpenHashMap<>(elementNum);

        // Deserialize the data
        for (int i = 0; i < elementNum; i++) {
            String key = ByteBufSerdeUtils.deserializeUTF8(buf);
            Serialize value = ByteBufSerdeUtils.deserializeObject(buf);
            data.put(key, (IElement)value);
        }
    }

    @Override
    public int bufferLen() {
        int dataLen = 0;

        // Element data
        for (Entry<String, IElement> entry : data.entrySet()) {
            dataLen += (ByteBufSerdeUtils.serializedUTF8Len(entry.getKey())
                    + ByteBufSerdeUtils.serializedObjectLen(entry.getValue()));
        }
        return super.bufferLen() + 4 + dataLen;
    }

    @Override
    public void serialize(DataOutputStream output) throws IOException {
        super.serialize(output);
        // Valid element number
        output.writeInt(data.size());

        // Element data
        int writeNum = 0;
        for (Entry<String, IElement> entry : data.entrySet()) {
            output.writeUTF(entry.getKey());
            entry.getValue().serialize(output);
            writeNum++;
        }
    }

    @Override
    public void deserialize(DataInputStream input) throws IOException {
        super.deserialize(input);

        // Valid element number
        int elementNum = input.readInt();
        data = new Object2ObjectOpenHashMap<>(elementNum);

        // Deserialize the data
        for (int i = 0; i < elementNum; i++) {
            IElement element = newElement();
            data.put(input.readUTF(), element);
            element.deserialize(input);
        }
    }

    @Override
    public int dataLen() {
        return bufferLen();
    }
}
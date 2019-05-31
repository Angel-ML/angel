package com.tencent.angel.graph.ps.storage.vector.storage;

import com.tencent.angel.ps.storage.vector.element.IElement;
import com.tencent.angel.ps.storage.vector.storage.LongElementStorage;
import io.netty.buffer.ByteBuf;

public class NodeArrayElementStorage extends LongElementStorage {
    @Override
    public void clear() {

    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public boolean isDense() {
        return false;
    }

    @Override
    public boolean isSparse() {
        return false;
    }

    @Override
    public boolean isSorted() {
        return false;
    }

    @Override
    public Object deepClone() {
        return null;
    }

    @Override
    public IElement get(long index) {
        return null;
    }

    @Override
    public void set(long index, IElement value) {

    }

    @Override
    public IElement[] get(long[] indices) {
        return new IElement[0];
    }

    @Override
    public void set(long[] indices, IElement[] values) {

    }

    @Override
    public boolean exist(long index) {
        return false;
    }

    @Override
    public void serialize(ByteBuf buf) {
        super.serialize(buf);
    }

    @Override
    public void deserialize(ByteBuf buf) {
        super.deserialize(buf);
    }

    @Override
    public int bufferLen() {
        return super.bufferLen();
    }
}

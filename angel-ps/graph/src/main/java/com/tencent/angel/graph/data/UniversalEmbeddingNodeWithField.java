package com.tencent.angel.graph.data;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.common.StreamSerdeUtils;
import com.tencent.angel.ps.storage.vector.element.IElement;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class UniversalEmbeddingNodeWithField implements IElement {
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     *  fieldId2Emb: pair of fieldId and embedding
     */
    public Int2ObjectOpenHashMap<UniversalEmbeddingNode> fieldId2Emb;

    /**
     *  fieldId2UpdateTimestampSecond: pair of fieldId and embedding
     */
    public Int2LongOpenHashMap fieldId2UpdateTimestampSecond;

    /**
     *  fieldId2Version: per-(node,field) update version counter.
     *  Incremented every time applyFieldGrad updates this (node, field) pair.
     *  Used by the get PSF to return the current version to the worker, which
     *  computes staleness τ = currentVersion(at push time) - pullVersion.
     *  Lazily initialised to avoid memory overhead when staleness tracking
     *  is not used.
     */
    public Int2IntOpenHashMap fieldId2Version;

    private long fieldId2EmbSize;


    public UniversalEmbeddingNodeWithField(float[] embeddings, int fieldId, float[] slotsValues, int numSlots) {
        fieldId2Emb = new Int2ObjectOpenHashMap<>();
        fieldId2Emb.put(fieldId, new UniversalEmbeddingNode(embeddings, slotsValues, numSlots));
        if (fieldId2UpdateTimestampSecond == null) {
            fieldId2UpdateTimestampSecond = new Int2LongOpenHashMap();
        }
    }

    public UniversalEmbeddingNodeWithField(float[] embeddings, int fieldId) {
        this(embeddings, fieldId, null, 1);
    }

    public UniversalEmbeddingNodeWithField() {
        this(null, -1, null, 1);
    }

    public UniversalEmbeddingNodeWithField(UniversalEmbeddingNode emb, int fieldId) {
        fieldId2Emb = new Int2ObjectOpenHashMap<>();
        fieldId2Emb.put(fieldId, emb);
        fieldId2UpdateTimestampSecond = null;
    }

    public UniversalEmbeddingNodeWithField(Int2ObjectOpenHashMap<UniversalEmbeddingNode> fieldId2Emb) {
        this.fieldId2Emb = fieldId2Emb;
        this.fieldId2UpdateTimestampSecond = null;
    }

    public UniversalEmbeddingNodeWithField(Int2ObjectOpenHashMap<UniversalEmbeddingNode> fieldId2Emb, Int2LongOpenHashMap fieldId2UpdateTimestampSecond) {
        this.fieldId2Emb = fieldId2Emb;
        this.fieldId2UpdateTimestampSecond = fieldId2UpdateTimestampSecond;
    }

    public UniversalEmbeddingNodeWithField(Int2LongOpenHashMap fieldId2UpdateTimestampSecond) {
        this.fieldId2UpdateTimestampSecond = fieldId2UpdateTimestampSecond;
    }

    public Int2ObjectOpenHashMap<UniversalEmbeddingNode> getFieldId2Emb() {
        return fieldId2Emb;
    }

    public void setFieldId2Emb(Int2ObjectOpenHashMap<UniversalEmbeddingNode> fieldId2Emb) {
        this.fieldId2Emb = fieldId2Emb;
    }

    public Int2LongOpenHashMap getFieldId2UpdateTimestampSecond() {
        if (fieldId2UpdateTimestampSecond == null) {
            fieldId2UpdateTimestampSecond = new Int2LongOpenHashMap();
        }
        return fieldId2UpdateTimestampSecond;
    }

    public void setFieldId2UpdateTimestampSecond(Int2LongOpenHashMap fieldId2UpdateTimestampSecond) {
        this.fieldId2UpdateTimestampSecond = fieldId2UpdateTimestampSecond;
    }

    /**
     * Get the per-field version map, lazily initialised.
     * Used for distributed staleness tracking.
     */
    public Int2IntOpenHashMap getFieldId2Version() {
        if (fieldId2Version == null) {
            fieldId2Version = new Int2IntOpenHashMap();
        }
        return fieldId2Version;
    }

    /**
     * Increment and return the version for a given fieldId.
     * Called by the update PSF after applyFieldGrad.
     */
    public int incrementAndGetVersion(int fieldId) {
        if (fieldId2Version == null) {
            fieldId2Version = new Int2IntOpenHashMap();
        }
        int newVersion = fieldId2Version.get(fieldId) + 1;
        fieldId2Version.put(fieldId, newVersion);
        return newVersion;
    }

    /**
     * Get the current version for a given fieldId (0 if never updated).
     * Called by the get PSF to return the version to the worker.
     */
    public int getVersion(int fieldId) {
        if (fieldId2Version == null) {
            return 0;
        }
        return fieldId2Version.get(fieldId);
    }

    public long getFieldId2EmbSize() {
        return fieldId2EmbSize;
    }

    public void setFieldId2EmbSize(long fieldId2EmbSize) {
        this.fieldId2EmbSize = fieldId2EmbSize;
    }

    @Override
    public Object deepClone() {
        // fieldId2Emb is nullable: the (Int2LongOpenHashMap) constructor never sets
        // it, and deserialize() leaves it null when the serialized size was 0.
        // Its values can be null too (see the (UniversalEmbeddingNode, int)
        // constructor). serialize/bufferLen already guard all of this, so the
        // clone must not be the only place that assumes non-null.
        Int2ObjectOpenHashMap<UniversalEmbeddingNode> fieldId2EmbClone = null;
        if (this.fieldId2Emb != null) {
            fieldId2EmbClone = new Int2ObjectOpenHashMap<>(fieldId2Emb.size());
            for (Int2ObjectMap.Entry<UniversalEmbeddingNode> entry: fieldId2Emb.int2ObjectEntrySet()) {
                int fieldId = entry.getIntKey();
                UniversalEmbeddingNode emb = entry.getValue();
                fieldId2EmbClone.put(fieldId, emb == null ? null : (UniversalEmbeddingNode) emb.deepClone());
            }
        }

        Int2LongOpenHashMap fieldId2UpdateTimeClone = null;
        if (this.fieldId2UpdateTimestampSecond != null) {
            fieldId2UpdateTimeClone = new Int2LongOpenHashMap();
            for (Int2LongMap.Entry entry: fieldId2UpdateTimestampSecond.int2LongEntrySet()) {
                fieldId2UpdateTimeClone.put(entry.getIntKey(), entry.getLongValue());
            }
        }

        Int2IntOpenHashMap fieldId2VersionClone = null;
        if (this.fieldId2Version != null) {
            fieldId2VersionClone = new Int2IntOpenHashMap();
            for (Int2IntMap.Entry entry: fieldId2Version.int2IntEntrySet()) {
                fieldId2VersionClone.put(entry.getIntKey(), entry.getIntValue());
            }
        }

        UniversalEmbeddingNodeWithField clone = new UniversalEmbeddingNodeWithField(fieldId2EmbClone, fieldId2UpdateTimeClone);
        clone.fieldId2Version = fieldId2VersionClone;
        return clone;
    }

    @Override
    public void serialize(ByteBuf output) {
        if (fieldId2Emb != null) {
            ByteBufSerdeUtils.serializeInt(output, fieldId2Emb.size());
            for (Int2ObjectMap.Entry<UniversalEmbeddingNode> entry: fieldId2Emb.int2ObjectEntrySet()) {
                ByteBufSerdeUtils.serializeInt(output, entry.getIntKey());
                entry.getValue().serialize(output);
            }
        } else {
            ByteBufSerdeUtils.serializeInt(output, 0);
        }

        if (fieldId2UpdateTimestampSecond != null) {
            ByteBufSerdeUtils.serializeInt(output, fieldId2UpdateTimestampSecond.size());
            for (Int2LongMap.Entry entry: fieldId2UpdateTimestampSecond.int2LongEntrySet()) {
                ByteBufSerdeUtils.serializeInt(output, entry.getIntKey());
                ByteBufSerdeUtils.serializeLong(output, entry.getLongValue());
            }
        } else {
            ByteBufSerdeUtils.serializeInt(output, 0);
        }

        if (fieldId2Version != null) {
            ByteBufSerdeUtils.serializeInt(output, fieldId2Version.size());
            for (Int2IntMap.Entry entry: fieldId2Version.int2IntEntrySet()) {
                ByteBufSerdeUtils.serializeInt(output, entry.getIntKey());
                ByteBufSerdeUtils.serializeInt(output, entry.getIntValue());
            }
        } else {
            ByteBufSerdeUtils.serializeInt(output, 0);
        }

    }

    @Override
    public void deserialize(ByteBuf input) {
        int size = ByteBufSerdeUtils.deserializeInt(input);
        if (size > 0) {
            fieldId2Emb = new Int2ObjectOpenHashMap<>(size);
            for (int i = 0; i < size; i++) {
                int fieldId = ByteBufSerdeUtils.deserializeInt(input);
                UniversalEmbeddingNode emb = new UniversalEmbeddingNode();
                emb.deserialize(input);
                fieldId2Emb.put(fieldId, emb);
            }
        }

        size = ByteBufSerdeUtils.deserializeInt(input);
        if (size > 0) {
            fieldId2UpdateTimestampSecond = new Int2LongOpenHashMap(size);
            for (int i = 0; i < size; i++) {
                int fieldId = ByteBufSerdeUtils.deserializeInt(input);
                long updateTime = ByteBufSerdeUtils.deserializeLong(input);
                fieldId2UpdateTimestampSecond.put(fieldId, updateTime);
            }
        }

        size = ByteBufSerdeUtils.deserializeInt(input);
        if (size > 0) {
            fieldId2Version = new Int2IntOpenHashMap(size);
            for (int i = 0; i < size; i++) {
                int fieldId = ByteBufSerdeUtils.deserializeInt(input);
                int version = ByteBufSerdeUtils.deserializeInt(input);
                fieldId2Version.put(fieldId, version);
            }
        }
    }

    @Override
    public int bufferLen() {
        int len = ByteBufSerdeUtils.INT_LENGTH * 3;
        if (fieldId2Emb != null) {
            for (Int2ObjectMap.Entry<UniversalEmbeddingNode> entry: fieldId2Emb.int2ObjectEntrySet()) {
                len += ByteBufSerdeUtils.INT_LENGTH;
                len += entry.getValue().bufferLen();
            }
        }

        if (fieldId2UpdateTimestampSecond != null) {
            len += (ByteBufSerdeUtils.INT_LENGTH + ByteBufSerdeUtils.LONG_LENGTH) * fieldId2UpdateTimestampSecond.size();
        }

        if (fieldId2Version != null) {
            len += (ByteBufSerdeUtils.INT_LENGTH + ByteBufSerdeUtils.INT_LENGTH) * fieldId2Version.size();
        }
        return len;
    }

    @Override
    public void serialize(DataOutputStream output) throws IOException {
        if (fieldId2Emb != null) {
            StreamSerdeUtils.serializeInt(output, fieldId2Emb.size());
            for (Int2ObjectMap.Entry<UniversalEmbeddingNode> entry: fieldId2Emb.int2ObjectEntrySet()) {
                StreamSerdeUtils.serializeInt(output, entry.getIntKey());
                entry.getValue().serialize(output);
            }
        } else {
            StreamSerdeUtils.serializeInt(output, 0);
        }

        if (fieldId2UpdateTimestampSecond != null) {
            StreamSerdeUtils.serializeInt(output, fieldId2UpdateTimestampSecond.size());
            for (Int2LongMap.Entry entry: fieldId2UpdateTimestampSecond.int2LongEntrySet()) {
                StreamSerdeUtils.serializeInt(output, entry.getIntKey());
                StreamSerdeUtils.serializeLong(output, entry.getLongValue());
            }
        } else {
            StreamSerdeUtils.serializeInt(output, 0);
        }

        if (fieldId2Version != null) {
            StreamSerdeUtils.serializeInt(output, fieldId2Version.size());
            for (Int2IntMap.Entry entry: fieldId2Version.int2IntEntrySet()) {
                StreamSerdeUtils.serializeInt(output, entry.getIntKey());
                StreamSerdeUtils.serializeInt(output, entry.getIntValue());
            }
        } else {
            StreamSerdeUtils.serializeInt(output, 0);
        }
    }

    @Override
    public void deserialize(DataInputStream input) throws IOException {
        int size = StreamSerdeUtils.deserializeInt(input);
        if (size > 0) {
            fieldId2Emb = new Int2ObjectOpenHashMap<>(size);
            for (int i = 0; i < size; i++) {
                int fieldId =StreamSerdeUtils.deserializeInt(input);
                UniversalEmbeddingNode emb = new UniversalEmbeddingNode();
                emb.deserialize(input);
                fieldId2Emb.put(fieldId, emb);
            }
        }

        size = StreamSerdeUtils.deserializeInt(input);
        if (size > 0) {
            fieldId2UpdateTimestampSecond = new Int2LongOpenHashMap(size);
            for (int i = 0; i < size; i++) {
                int fieldId = StreamSerdeUtils.deserializeInt(input);
                long updateTime = StreamSerdeUtils.deserializeLong(input);
                fieldId2UpdateTimestampSecond.put(fieldId, updateTime);
            }
        }

        size = StreamSerdeUtils.deserializeInt(input);
        if (size > 0) {
            fieldId2Version = new Int2IntOpenHashMap(size);
            for (int i = 0; i < size; i++) {
                int fieldId = StreamSerdeUtils.deserializeInt(input);
                int version = StreamSerdeUtils.deserializeInt(input);
                fieldId2Version.put(fieldId, version);
            }
        }
    }

    @Override
    public int dataLen() {
        return bufferLen();
    }

    public ReentrantReadWriteLock getLock() {
        return lock;
    }

    public void lockRead() {
        lock.readLock().lock();
    }

    public void unlockRead() {
        lock.readLock().unlock();
    }

    public void lockWrite() {
        lock.writeLock().lock();
    }

    public void unlockWrite() {
        lock.writeLock().unlock();
    }
}

package com.tencent.angel.spark.ml.ann_core.psf.getSearchResult;

import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.spark.ml.ann_core.NeighborDistance;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

public class GetSearchResultPartResult extends PartitionGetResult {
    private Long2ObjectOpenHashMap<NeighborDistance[]> resKeysDis;


    public GetSearchResultPartResult(Long2ObjectOpenHashMap<NeighborDistance[]> resKeysDis) {
        this.resKeysDis = resKeysDis;
    }

    public GetSearchResultPartResult() {
        this(null);
    }

    public Long2ObjectOpenHashMap<NeighborDistance[]> getResKeysDis() {
        return this.resKeysDis;
    }

    public int size() {
        return this.resKeysDis.size();
    }

    @Override
    public void serialize(ByteBuf output) {
        output.writeInt(resKeysDis.size());
        ObjectIterator<Long2ObjectOpenHashMap.Entry<NeighborDistance[]>> keyIter =
                resKeysDis.long2ObjectEntrySet().fastIterator();
        while (keyIter.hasNext()) {
            Long2ObjectOpenHashMap.Entry<NeighborDistance[]> entry = keyIter.next();
            output.writeLong(entry.getLongKey());
            NeighborDistance[] value = entry.getValue();
            int num = value.length;
            output.writeInt(num);
            for (int i = 0; i < num; i += 1) {
                output.writeLong(value[i].getNeighbor());
                output.writeFloat(value[i].getDistance());
            }
        }
    }

    @Override
    public void deserialize(ByteBuf input) {
        int len = input.readInt();
        this.resKeysDis = new Long2ObjectOpenHashMap<>(len);

        for (int i = 0; i < len; i += 1) {
            long key = input.readLong();
            int num = input.readInt();
            NeighborDistance[] value = new NeighborDistance[num];
            for (int j = 0; j < num; j += 1) {
                long k = input.readLong();
                float v = input.readFloat();
                value[j] = new NeighborDistance(k, v);
            }
            this.resKeysDis.put(key, value);
        }
    }

    @Override
    public int bufferLen() {
        int len = 4;
        ObjectIterator<Long2ObjectOpenHashMap.Entry<NeighborDistance[]>> keyIter =
                resKeysDis.long2ObjectEntrySet().fastIterator();
        while (keyIter.hasNext()) {
            Long2ObjectOpenHashMap.Entry<NeighborDistance[]> entry = keyIter.next();
            NeighborDistance[] value = entry.getValue();
            int num = value.length;
            len += 12;
            for (int i = 0; i < num; i += 1) {
                len += 12;
            }
        }
        return len;
    }

}

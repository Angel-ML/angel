package com.tencent.angel.graph.embedding.node2vec;

import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import scala.Tuple3;

public class PullAliasPartitionResult extends PartitionGetResult {
    private Long2ObjectOpenHashMap<Tuple3<long[], float[], int[]>> partResult;

    public PullAliasPartitionResult(Long2ObjectOpenHashMap<Tuple3<long[], float[], int[]>> part) {
        super();
        this.partResult = part;
    }

    public PullAliasPartitionResult() {
        super();
    }

    public Long2ObjectOpenHashMap<Tuple3<long[], float[], int[]>> getPartResult() {
        return partResult;
    }

    @Override
    public void serialize(ByteBuf output) {
        int size = partResult.size();
        output.writeInt(size);
        long[] keySet = partResult.keySet().toLongArray();
        for (int i = 0; i < keySet.length; i++) {
            long key = keySet[i];
            output.writeLong(key);
            int len = partResult.get(key)._1().length;
            long[] neigh = partResult.get(key)._1();
            float[] accept = partResult.get(key)._2();
            int[] alias = partResult.get(key)._3();
            output.writeInt(len);
            for (int j = 0; j < len; j++) {
                output.writeLong(neigh[j]);
                output.writeFloat(accept[j]);
                output.writeInt(alias[j]);
            }
        }
    }

    @Override
    public void deserialize(ByteBuf input) {
        int size = input.readInt();
        partResult = new Long2ObjectOpenHashMap<>(size);
        for (int i = 0; i < size; i++) {
            long key = input.readLong();
            int len = input.readInt();
            long[] neigh = new long[len];
            float[] accept = new float[len];
            int[] alias = new int[len];
            for (int j = 0; j < len; j++) {
                neigh[j] = input.readLong();
                accept[j] = input.readFloat();
                alias[j] = input.readInt();
            }

            partResult.put(key, new Tuple3<>(neigh, accept, alias));
        }

    }

    @Override
    public int bufferLen() {
        int len = 4;
        long[] keySet = partResult.keySet().toLongArray();
        for (int i = 0; i < partResult.size(); i++) {
            len += 8 + 4;
            long key = keySet[i];
            for (int j = 0; j < partResult.get(key)._1().length; j++) {
                len += 8 + 4 + 4;
            }
        }
        return len;
    }
}

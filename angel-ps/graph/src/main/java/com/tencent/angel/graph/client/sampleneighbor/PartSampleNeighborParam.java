package com.tencent.angel.graph.client.sampleneighbor;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import io.netty.buffer.ByteBuf;

public class PartSampleNeighborParam extends PartitionGetParam {
    /**
     * Node ids: it just a view for original node ids
     */
    private long[] nodeIds;

    /**
     * Edge types
     */
    private int[] edgeTypes;

    private int count;

    /**
     * Store position: start index in nodeIds
     */
    private int startIndex;

    /**
     * Store position: end index in nodeIds
     */
    private int endIndex;

    public PartSampleNeighborParam(int matrixId, PartitionKey part, long[] nodeIds, int[] edgeTypes, int count
            , int startIndex, int endIndex) {
        super(matrixId, part);
        this.nodeIds = nodeIds;
        this.edgeTypes = edgeTypes;
        this.count = count;
        this.startIndex = startIndex;
        this.endIndex = endIndex;
    }

    public PartSampleNeighborParam() {
        this(0, null, null, null, 0, 0, 0);
    }

    public long[] getNodeIds() {
        return nodeIds;
    }

    public int[] getEdgeTypes() {
        return edgeTypes;
    }

    public int getCount() {
        return count;
    }

    public int getStartIndex() {
        return startIndex;
    }

    public int getEndIndex() {
        return endIndex;
    }

    @Override
    public void serialize(ByteBuf buf) {
        super.serialize(buf);
        buf.writeInt(endIndex - startIndex);

        for (int i = startIndex; i < endIndex; i++) {
            buf.writeLong(nodeIds[i]);
        }

        if (edgeTypes == null) {
            buf.writeInt(0);
        } else {
            buf.writeInt(edgeTypes.length);
            for (int i = 0; i < edgeTypes.length; i++) {
                buf.writeInt(edgeTypes[i]);
            }
        }

        buf.writeInt(count);
    }

    @Override
    public void deserialize(ByteBuf buf) {
        super.deserialize(buf);
        int len = buf.readInt();
        nodeIds = new long[len];
        for (int i = 0; i < len; i++) {
            nodeIds[i] = buf.readLong();
        }

        len = buf.readInt();
        edgeTypes = new int[len];
        for (int i = 0; i < len; i++) {
            edgeTypes[i] = buf.readInt();
        }

        count = buf.readInt();
    }

    @Override
    public int bufferLen() {
        return super.bufferLen() + 4 + 8 * nodeIds.length + 4 + ((edgeTypes == null) ? 0
                : 4 * edgeTypes.length) + 4;
    }
}

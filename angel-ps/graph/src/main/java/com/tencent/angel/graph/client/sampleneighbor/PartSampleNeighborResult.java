package com.tencent.angel.graph.client.sampleneighbor;

import com.tencent.angel.graph.client.NodeIDWeightPairs;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;

public class PartSampleNeighborResult extends PartitionGetResult {
    private int partId;
    private NodeIDWeightPairs[] nodeIdsWeights;
    private static final int NullMark = 0;
    private static final int NotNullMark = 1;

    PartSampleNeighborResult(int partId, NodeIDWeightPairs[] nodeIdsWeights) {
        this.partId = partId;
        this.nodeIdsWeights = nodeIdsWeights;
    }

    public PartSampleNeighborResult() {
        this(-1, null);
    }

    @Override
    public void serialize(ByteBuf buf) {
        buf.writeInt(partId);
        buf.writeInt(nodeIdsWeights.length);
        for (int i = 0; i < nodeIdsWeights.length; i++) {
            if (nodeIdsWeights[i] == null) {
                buf.writeInt(NullMark);
            } else {
                buf.writeInt(NotNullMark);
                nodeIdsWeights[i].serialize(buf);
            }
        }
    }

    @Override
    public void deserialize(ByteBuf buf) {
        partId = buf.readInt();
        int size = buf.readInt();
        nodeIdsWeights = new NodeIDWeightPairs[size];
        for (int i = 0; i < size; i++) {
            int mark = buf.readInt();
            if (mark == NotNullMark) {
                nodeIdsWeights[i] = new NodeIDWeightPairs();
                nodeIdsWeights[i].deserialize(buf);
            }
        }
    }

    @Override
    public int bufferLen() {
        int len = 8;
        for (int i = 0; i < nodeIdsWeights.length; i++) {
            if (nodeIdsWeights[i] == null) {
                len += 4;
            } else {
                len += 4;
                len += nodeIdsWeights[i].bufferLen();
            }
        }

        return len;
    }

    public NodeIDWeightPairs[] getNeighborIndices() {
        return nodeIdsWeights;
    }

    public int getPartId() {
        return partId;
    }
}

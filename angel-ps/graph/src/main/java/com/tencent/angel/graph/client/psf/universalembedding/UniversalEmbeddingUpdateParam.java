package com.tencent.angel.graph.client.psf.universalembedding;


import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam;
import com.tencent.angel.ps.storage.vector.element.IElement;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.transport.router.KeyValuePart;
import com.tencent.angel.psagent.matrix.transport.router.RouterUtils;
import java.util.ArrayList;
import java.util.List;

/**
 * The parameter of embedding matrix for update
 */
public class UniversalEmbeddingUpdateParam extends UpdateParam {

    private long[] nodeIds;

    private IElement[] grads;

    private float[] floats;

    private int[] ints;

    public UniversalEmbeddingUpdateParam(int matrixId, long[] nodeIds, IElement[] grads,
                                         float[] floats, int[] ints) {
        super(matrixId);
        this.nodeIds = nodeIds;
        this.grads = grads;
        this.floats = floats;
        this.ints = ints;
    }

    public UniversalEmbeddingUpdateParam() {
        this(-1, null, null, null, null);
    }

    public long[] getNodeIds() {
        return nodeIds;
    }

    public IElement[] getGrads() {
        return grads;
    }

    public float[] getFloats() {
        return floats;
    }

    public int[] getInts() {
        return ints;
    }

    @Override
    public List<PartitionUpdateParam> split() {
        // Get matrix meta
        MatrixMeta meta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
        PartitionKey[] partitions = meta.getPartitionKeys();

        // Split nodeIds
        KeyValuePart[] splits = RouterUtils.split(meta, 0, nodeIds, grads);
        assert partitions.length == splits.length;

        // Generate rpc params
        List<PartitionUpdateParam> partParams = new ArrayList<>(partitions.length);
        for(int i = 0; i < partitions.length; i++) {
            if(splits[i] != null && splits[i].size() > 0) {
                partParams.add(new PartUniversalEmbeddingUpdateParam(matrixId, partitions[i], splits[i], floats, ints));
            }
        }

        return partParams;
    }
}
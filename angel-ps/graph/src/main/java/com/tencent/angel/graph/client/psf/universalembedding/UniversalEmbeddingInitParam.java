package com.tencent.angel.graph.client.psf.universalembedding;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.transport.router.KeyPart;
import com.tencent.angel.psagent.matrix.transport.router.RouterUtils;
import java.util.ArrayList;
import java.util.List;

/**
 * The parameter of embedding matrix random init
 */
public class UniversalEmbeddingInitParam extends UpdateParam {

    private long[] nodeIds;

    private int seed;

    private int dim;

    private int numSlots;

    private float[] floats;

    private int [] ints;

    public UniversalEmbeddingInitParam(int matrixId, long[] nodeIds, int seed, int dim, int numSlots,
                                       float[] floats, int[] ints) {
        super(matrixId);
        this.nodeIds = nodeIds;
        this.seed =seed;
        this.dim = dim;
        this.numSlots = numSlots;
        this.floats = floats;
        this.ints = ints;
    }

    public UniversalEmbeddingInitParam(int matrixId, long[] nodeIds, int dim) {
        this(matrixId, nodeIds, -1, dim, 1, new float[0], new int[]{dim});
    }


    public UniversalEmbeddingInitParam() {
        this(-1, null, -1,-1, -1, null, null);
    }

    @Override
    public List<PartitionUpdateParam> split() {
        // Get matrix meta
        MatrixMeta meta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
        PartitionKey[] partitions = meta.getPartitionKeys();

        // Split nodeIds
        KeyPart[] splits = RouterUtils.split(meta, 0, nodeIds);
        assert partitions.length == splits.length;

        // Generate rpc params
        List<PartitionUpdateParam> partParams = new ArrayList<>(partitions.length);
        for(int i = 0; i < partitions.length; i++) {
            if(splits[i] != null && splits[i].size() > 0) {
                partParams.add(new PartUniversalEmbeddingInitParam(matrixId, partitions[i], splits[i],
                        seed, dim, numSlots, floats, ints));
            }
        }

        return partParams;
    }
}

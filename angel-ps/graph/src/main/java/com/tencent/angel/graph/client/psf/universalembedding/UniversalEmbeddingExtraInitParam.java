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

public class UniversalEmbeddingExtraInitParam extends UpdateParam {

    private long[] nodeIds;

    private IElement[] embeddings;

    private int dim;

    private int numSlots;

    public UniversalEmbeddingExtraInitParam(int matrixId, long[] nodeIds, IElement[] embeddings,
                                            int dim, int numSlots) {
        super(matrixId);
        this.nodeIds = nodeIds;
        this.embeddings = embeddings;
        this.dim = dim;
        this.numSlots = numSlots;
    }

    @Override
    public List<PartitionUpdateParam> split() {
        MatrixMeta meta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
        PartitionKey[] parts = meta.getPartitionKeys();

        KeyValuePart[] splits = RouterUtils.split(meta, 0, nodeIds, embeddings);
        assert parts.length == splits.length;

        List<PartitionUpdateParam> partParams = new ArrayList<>(parts.length);
        for(int i = 0; i < parts.length; i++) {
            if(splits[i] != null && splits[i].size() > 0) {
                partParams.add(new PartUniversalEmbeddingExtraInitParam(matrixId, parts[i], splits[i],
                        dim, numSlots));
            }
        }

        return partParams;
    }
}

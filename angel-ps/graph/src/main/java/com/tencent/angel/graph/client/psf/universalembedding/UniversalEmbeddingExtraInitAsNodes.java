package com.tencent.angel.graph.client.psf.universalembedding;

import com.tencent.angel.graph.data.EmbeddingOrGrad;
import com.tencent.angel.graph.data.UniversalEmbeddingNode;
import com.tencent.angel.graph.utils.GraphMatrixUtils;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.ps.storage.vector.element.IElement;
import com.tencent.angel.psagent.matrix.transport.router.operator.ILongKeyAnyValuePartOp;

/**
 * A PS function to initialize the universal embedding matrix on PS with extra embeddings
 */
public class UniversalEmbeddingExtraInitAsNodes extends UpdateFunc {
    /**
     * Create a new UpdateParam
     */
    public UniversalEmbeddingExtraInitAsNodes(UniversalEmbeddingExtraInitParam param) {
        super(param);
    }

    public UniversalEmbeddingExtraInitAsNodes() {
        this(null);
    }

    @Override
    public void partitionUpdate(PartitionUpdateParam partParam) {
        PartUniversalEmbeddingExtraInitParam param = (PartUniversalEmbeddingExtraInitParam) partParam;
        ServerLongAnyRow row = GraphMatrixUtils.getPSLongKeyRow(psContext, param);
        ILongKeyAnyValuePartOp keyValuePart = (ILongKeyAnyValuePartOp) param.getKeyValuePart();
        int dim = param.getDim();
        int numSlots = param.getNumSlots();
        long[] nodeIds = keyValuePart.getKeys();
        IElement[] embeddings = keyValuePart.getValues();
        row.startWrite();
        try {
            for (int i = 0; i < nodeIds.length; i++) {
                UniversalEmbeddingNode embeddingNode = (UniversalEmbeddingNode) row.get(nodeIds[i]);
                if (embeddingNode == null) {
                    if (numSlots <= 1) {
                        embeddingNode = new UniversalEmbeddingNode(((EmbeddingOrGrad)embeddings[i])
                                .getValues());
                    } else {
                        float[] slotsValues = new float[dim * (numSlots - 1)];
                        embeddingNode = new UniversalEmbeddingNode(((EmbeddingOrGrad)embeddings[i])
                                .getValues(), slotsValues, numSlots);
                    }
                    row.set(nodeIds[i], embeddingNode);
                } else {
                    embeddingNode.setEmbeddings(((EmbeddingOrGrad)embeddings[i])
                            .getValues());
                }
            }
        } finally {
            row.endWrite();
        }
    }
}
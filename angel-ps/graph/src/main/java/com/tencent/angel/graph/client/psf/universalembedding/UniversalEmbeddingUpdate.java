package com.tencent.angel.graph.client.psf.universalembedding;

import com.tencent.angel.exception.InvalidParameterException;
import com.tencent.angel.graph.data.EmbeddingOrGrad;
import com.tencent.angel.graph.data.UniversalEmbeddingNode;
import com.tencent.angel.graph.utils.GraphMatrixUtils;
import com.tencent.angel.ml.matrix.psf.update.base.GeneralPartUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.ps.storage.vector.element.IElement;
import com.tencent.angel.psagent.matrix.transport.router.operator.ILongKeyAnyValuePartOp;

/**
 * update func for embedding matrix
 */
public class UniversalEmbeddingUpdate extends UpdateFunc {

    /**
     * Create a new UpdateParam
     */
    public UniversalEmbeddingUpdate(UniversalEmbeddingUpdateParam param) {
        super(param);
    }

    public UniversalEmbeddingUpdate() {
        this(null);
    }

    @Override
    public void partitionUpdate(PartitionUpdateParam partParam) {
        GeneralPartUpdateParam initParam = (PartUniversalEmbeddingUpdateParam) partParam;
        ServerLongAnyRow row = GraphMatrixUtils.getPSLongKeyRow(psContext, initParam);

        // Get node ids and grads
        ILongKeyAnyValuePartOp split = (ILongKeyAnyValuePartOp) initParam.getKeyValuePart();
        long[] nodeIds = split.getKeys();
        IElement[] grads = split.getValues();
        float[] floats = ((PartUniversalEmbeddingUpdateParam) initParam).getFloats();
        int[] ints = ((PartUniversalEmbeddingUpdateParam) initParam).getInts();
        int optimType = ints[ints.length-1];
        row.startWrite();
        try {
            for(int i = 0; i < nodeIds.length; i++) {
                UniversalEmbeddingNode embeddingNode = (UniversalEmbeddingNode) row.get(nodeIds[i]);
                if (embeddingNode == null) continue;
                //0-sgd, 1-momentum, 2-adam, 3-adadelta
                if (optimType == 0) {
                    sgdUpdate(embeddingNode.getEmbeddings(), ((EmbeddingOrGrad)grads[i]).getValues(),
                            floats, ints);
                } else if (optimType == 1) {
                    momentumUpdate(embeddingNode.getEmbeddings(), ((EmbeddingOrGrad)grads[i]).getValues(),
                            embeddingNode.getSlotsValues(), floats, ints);
                } else if (optimType == 2) {
                    adamUpdate(embeddingNode.getEmbeddings(), ((EmbeddingOrGrad)grads[i]).getValues(),
                            embeddingNode.getSlotsValues(), floats, ints);
                } else if (optimType == 3) {
                    adaDeltaUpdate(embeddingNode.getEmbeddings(), ((EmbeddingOrGrad)grads[i]).getValues(),
                            embeddingNode.getSlotsValues(), floats, ints);
                } else {
                    throw new InvalidParameterException("Unsupported optimType " + optimType);
                }
            }
        } finally {
            row.endWrite();
        }
    }

    public void sgdUpdate(float[] embedding, float[] grads, float[] floats, int[] ints) {
        float stepSize = floats[0];
        for (int i = 0; i < embedding.length; i++) {
            embedding[i] += -1.0f * stepSize * grads[i];
        }
    }

    public void momentumUpdate(float[] embedding, float[] grad, float[] slotValues, float[] floats,
                               int[] ints) {
        float eta = floats[0];
        float momentum = floats[1];
        for (int i = 0; i < embedding.length; i++) {
            slotValues[i] = momentum * slotValues[i] + eta * grad[i];
            embedding[i] += -1.0f * slotValues[i];
        }

    }

    public void adamUpdate(float[] embedding, float[] grad, float[] slotValues, float[] floats,
                           int[] ints){
        float eta = floats[0];
        float gamma = floats[1];
        float beta = floats[2];
        float epsilon = floats[3];
        float numSteps = ints[0];
        int dim = embedding.length;
        for (int i = 0; i < dim; i++) {
            slotValues[i] = beta * slotValues[i] + (1 - beta) * grad[i];
            slotValues[i + dim] = gamma * slotValues[i + dim] + (1 - gamma) * grad[i] * grad[i];
            embedding[i] += -1.0f * eta * (slotValues[i] / (1 - Math.pow(beta, numSteps)))
                    / (Math.sqrt(slotValues[i + dim] / (1 - Math.pow(gamma, numSteps))) + epsilon);
        }
    }

    public void adaDeltaUpdate(float[] embedding, float[] grad, float[] slotValues, float[] floats,
                               int[] ints) {
        float eta = floats[0];
        float factor = floats[1];
        float epsilon = floats[2];
        for (int i = 0; i < embedding.length; i++) {
            slotValues[i] = factor * slotValues[i] + (1 - factor) * grad[i] * grad[i];
            embedding[i] += -1.0f * eta / Math.sqrt(slotValues[i] + epsilon) * grad[i];
        }
    }
}

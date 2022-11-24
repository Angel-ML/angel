package com.tencent.angel.graph.client.psf.universalembedding;

import com.tencent.angel.graph.data.UniversalEmbeddingNode;
import com.tencent.angel.graph.utils.GraphMatrixUtils;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.psagent.matrix.transport.router.operator.ILongKeyPartOp;

import java.util.Random;

/**
 * A PS function to initialize the universal embedding matrix on PS
 */
public class UniversalEmbeddingInitAsNodes extends UpdateFunc {
    /**
     * Create a new UpdateParam
     */
    public UniversalEmbeddingInitAsNodes(UniversalEmbeddingInitParam param) {
        super(param);
    }

    public UniversalEmbeddingInitAsNodes() {
        this(null);
    }

    @Override
    public void partitionUpdate(PartitionUpdateParam partParam) {
        PartUniversalEmbeddingInitParam param = (PartUniversalEmbeddingInitParam) partParam;
        ServerLongAnyRow row = GraphMatrixUtils.getPSLongKeyRow(psContext, param);

        ILongKeyPartOp split = (ILongKeyPartOp) param.getIndicesPart();
        int dim = param.getDim();
        int seed = param.getSeed();
        Random rand = new Random(seed);
        int numSlots = param.getNumSlots();
        float[] floats = param.getFloats();
        int[] ints = param.getInts();
        int iniFuncType = ints[ints.length-1];
        long[] nodeIds =split.getKeys();
        row.startWrite();
        try {
            for (long nodeId : nodeIds) {
                if (row.get(nodeId) != null) continue;
                UniversalEmbeddingNode embeddingNode;
                float[] embeddings = new float[dim];
                // 0-RandomUniform, 1-RandomNormal, 2-XavierOrKaimingUniform, 3-XavierOrKaimingNormal
                if (iniFuncType == 0) {
                    randomUniform(rand, embeddings, floats, ints);
                } else if (iniFuncType == 1) {
                    randomNormal(rand, embeddings, floats, ints);
                } else if (iniFuncType == 2) {
                    xavierOrKaimingUniform(rand, embeddings, floats, ints);
                } else if (iniFuncType == 3) {
                    xavierOrKaimingNormal(rand, embeddings, floats, ints);
                }
                if (numSlots <= 1) {
                    embeddingNode = new UniversalEmbeddingNode(embeddings);

                } else {
                    float[] slotsValues = new float[dim * (numSlots - 1)];
                    embeddingNode = new UniversalEmbeddingNode(embeddings, slotsValues, numSlots);
                }
                row.set(nodeId, embeddingNode);
            }
        } finally {
            row.endWrite();
        }
    }

    public void randomUniform(Random rand, float[] embeddings, float[] floats, int[] ints) {
        float min = floats[0];
        float max = floats[1];
        int scalingFactor = ints[0];
        for (int i = 0; i < embeddings.length; i++) {
            embeddings[i] = ((max - min) * rand.nextFloat() + min) / scalingFactor;
        }
    }

    public void randomNormal(Random rand, float[] embeddings, float[] floats, int[] ints) {
        float mean = floats[0];
        float stdDev = floats[1];
        int scalingFactor = ints[0];
        for (int i = 0; i < embeddings.length; i++) {
            embeddings[i] = (float) (stdDev * rand.nextGaussian() + mean) / scalingFactor;
        }
    }

    public void xavierOrKaimingUniform(Random rand, float[] embeddings, float[] floats, int[] ints) {
        //for Xavier, a = gain * math.sqrt(6.0 / (fin + fout))
        //for Kaiming, a = gain * math.sqrt(6.0 / fin)
        float a = floats[0];
        for (int i = 0; i < embeddings.length; i++) {
            embeddings[i] = rand.nextFloat() * 2 * a - a;
        }
    }

    public void xavierOrKaimingNormal(Random rand, float[] embeddings, float[] floats, int[] ints) {
        //for Xavier, a = gain * math.sqrt(2.0 / (fin + fout))
        //for Kaiming, a = gain *math.sqrt(2.0 / fin)
        float a = floats[0];
        for (int i = 0; i < embeddings.length; i++) {
            embeddings[i] = (float) (a * rand.nextGaussian());
        }
    }
}

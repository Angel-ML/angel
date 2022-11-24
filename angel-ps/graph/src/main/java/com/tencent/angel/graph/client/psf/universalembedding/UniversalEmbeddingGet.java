package com.tencent.angel.graph.client.psf.universalembedding;

import com.tencent.angel.exception.InvalidParameterException;
import com.tencent.angel.graph.client.psf.get.utils.GetFloatArrayAttrsResult;
import com.tencent.angel.graph.client.psf.get.utils.GetNodeAttrsParam;
import com.tencent.angel.graph.client.psf.get.utils.PartGetFloatArrayAttrsResult;
import com.tencent.angel.graph.data.UniversalEmbeddingNode;
import com.tencent.angel.graph.utils.GraphMatrixUtils;
import com.tencent.angel.ml.matrix.psf.get.base.GeneralPartGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.psagent.matrix.transport.router.KeyPart;
import com.tencent.angel.psagent.matrix.transport.router.operator.ILongKeyPartOp;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import java.util.List;

/**
 * A PS function to get the universal embedding matrix on PS
 */
public class UniversalEmbeddingGet extends GetFunc {

    /**
     * Create a get node embedding func
     *
     * @param param parameter of get udf
     */
    public UniversalEmbeddingGet(GetNodeAttrsParam param) {
        super(param);
    }

    public UniversalEmbeddingGet() {
        this(null);
    }

    @Override
    public PartitionGetResult partitionGet(PartitionGetParam partParam) {
        GeneralPartGetParam param = (GeneralPartGetParam) partParam;
        KeyPart keyPart = param.getIndicesPart();

        switch (keyPart.getKeyType()) {
            case LONG: {
                // Long type node id
                long[] nodeIds = ((ILongKeyPartOp) keyPart).getKeys();
                ServerLongAnyRow row = GraphMatrixUtils.getPSLongKeyRow(psContext, param);

                Long2ObjectOpenHashMap<float[]> nodeIdToEmbeddings =
                        new Long2ObjectOpenHashMap<>(nodeIds.length);
                for (long nodeId : nodeIds) {
                    if (row.get(nodeId) == null) {
                        // If node not exist, just skip
                        continue;
                    }
                    float[] embedding = ((UniversalEmbeddingNode) (row.get(nodeId))).getEmbeddings();
                    if (embedding != null) {
                        nodeIdToEmbeddings.put(nodeId, embedding);
                    }
                }
                return new PartGetFloatArrayAttrsResult(param.getPartKey().getPartitionId(), nodeIdToEmbeddings);
            }

            default: {
                // TODO: support String, Int, and Any type node id
                throw new InvalidParameterException("Unsupported index type " + keyPart.getKeyType());
            }
        }
    }

    @Override
    public GetResult merge(List<PartitionGetResult> partResults) {
        Long2ObjectOpenHashMap<float[]> nodeIdToEmbeddings =
                new Long2ObjectOpenHashMap<>(((GetNodeAttrsParam) param).getNodeIds().length);
        for (PartitionGetResult partitionGetResult : partResults) {
            Long2ObjectOpenHashMap<float[]> partNodeIdToFeats =
                    ((PartGetFloatArrayAttrsResult) partitionGetResult).getNodeIdToContents();
            if (partNodeIdToFeats != null) {
                nodeIdToEmbeddings.putAll(partNodeIdToFeats);
            }
        }
        return new GetFloatArrayAttrsResult(nodeIdToEmbeddings);
    }
}

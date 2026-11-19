package com.tencent.angel.graph.client.psf.universalembedding.adsEmbedding;

import com.tencent.angel.graph.client.psf.universalembedding.UniversalEmbeddingExtraInitAsNodes;
import com.tencent.angel.graph.data.EmbeddingOrGrad;
import com.tencent.angel.graph.data.UniversalEmbeddingNode;
import com.tencent.angel.graph.data.UniversalEmbeddingNodeWithField;
import com.tencent.angel.graph.utils.GraphMatrixUtils;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.ps.storage.vector.element.IElement;
import com.tencent.angel.psagent.matrix.transport.router.operator.ILongKeyAnyValuePartOp;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

public class UniversalEmbeddingWithFieldExtraInitAsNodes extends
    UniversalEmbeddingExtraInitAsNodes {

  public UniversalEmbeddingWithFieldExtraInitAsNodes(
      UniversalEmbeddingWithFieldExtraInitParam param) {
    super(param);
  }

  public UniversalEmbeddingWithFieldExtraInitAsNodes() {
    this(null);
  }

  @Override
  public void partitionUpdate(PartitionUpdateParam partParam) {
    PartUniversalEmbeddingWithFieldExtraInitParam param = (PartUniversalEmbeddingWithFieldExtraInitParam) partParam;
    ServerLongAnyRow row = GraphMatrixUtils.getPSLongKeyRow(psContext, param);
    ILongKeyAnyValuePartOp keyValuePart = (ILongKeyAnyValuePartOp) param.getKeyValuePart();
    int dim = param.getDim();
    int numSlots = param.getNumSlots();
    long[] nodeIds = keyValuePart.getKeys();
    IElement[] embeddings = keyValuePart.getValues();
    int fieldId = param.getFieldId();

    row.startWrite();
    try {
      for (int i = 0; i < nodeIds.length; i++) {
        UniversalEmbeddingNodeWithField embeddingNode = (UniversalEmbeddingNodeWithField) row
            .get(nodeIds[i]);
        if (embeddingNode == null || embeddingNode.getFieldId2Emb().get(fieldId) == null) {
          UniversalEmbeddingNode embedding;
          if (numSlots <= 1) {
            embedding = new UniversalEmbeddingNode(((EmbeddingOrGrad) embeddings[i])
                .getValues());
          } else {
            float[] slotsValues = new float[dim * (numSlots - 1)];
            embedding = new UniversalEmbeddingNode(((EmbeddingOrGrad) embeddings[i])
                .getValues(), slotsValues, numSlots);
          }
          if (embeddingNode == null) {
            embeddingNode = new UniversalEmbeddingNodeWithField();
            Int2ObjectOpenHashMap<UniversalEmbeddingNode> fieldId2Emb = new Int2ObjectOpenHashMap<>();
            embeddingNode.setFieldId2Emb(fieldId2Emb);
          }
          Int2ObjectOpenHashMap<UniversalEmbeddingNode> fieldId2Emb = embeddingNode
              .getFieldId2Emb();
          fieldId2Emb.put(fieldId, embedding);
          embeddingNode.setFieldId2Emb(fieldId2Emb);
          row.set(nodeIds[i], embeddingNode);
        } else {
          Int2ObjectOpenHashMap<UniversalEmbeddingNode> fieldId2Emb = embeddingNode
              .getFieldId2Emb();
          fieldId2Emb.get(fieldId).setEmbeddings(((EmbeddingOrGrad) embeddings[i]).getValues());

          embeddingNode.setFieldId2Emb(fieldId2Emb);
        }
      }
    } finally {
      row.endWrite();
    }
  }
}

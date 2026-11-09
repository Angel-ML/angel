package com.tencent.angel.graph.client.psf.universalembedding.adsEmbedding;

import com.tencent.angel.exception.InvalidParameterException;
import com.tencent.angel.graph.client.psf.universalembedding.PartUniversalEmbeddingUpdateParam;
import com.tencent.angel.graph.client.psf.universalembedding.UniversalEmbeddingUpdate;
import com.tencent.angel.graph.client.psf.universalembedding.UniversalEmbeddingUpdateParam;
import com.tencent.angel.graph.data.EmbeddingOrGradWithField;
import com.tencent.angel.graph.data.UniversalEmbeddingNode;
import com.tencent.angel.graph.data.UniversalEmbeddingNodeWithField;
import com.tencent.angel.graph.utils.GraphMatrixUtils;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.ps.storage.vector.element.IElement;
import com.tencent.angel.psagent.matrix.transport.router.operator.ILongKeyAnyValuePartOp;

public class UniversalEmbeddingWithFieldUpdate extends UniversalEmbeddingUpdate {

  public UniversalEmbeddingWithFieldUpdate(UniversalEmbeddingUpdateParam param) {
    super(param);
  }

  public UniversalEmbeddingWithFieldUpdate() {
    this(null);
  }

  @Override
  public void partitionUpdate(PartitionUpdateParam partParam) {
    PartUniversalEmbeddingUpdateParam param = (PartUniversalEmbeddingUpdateParam) partParam;
    ServerLongAnyRow row = GraphMatrixUtils.getPSLongKeyRow(psContext, param);

    ILongKeyAnyValuePartOp split = (ILongKeyAnyValuePartOp) param.getKeyValuePart();
    long[] nodeIds = split.getKeys();
    IElement[] grads = split.getValues();
    float[] floats = param.getFloats();
    int[] ints = param.getInts();
    int optimType = ints[ints.length - 1];

    row.startWrite();
    try {
      for (int i = 0; i < nodeIds.length; i++) {
        UniversalEmbeddingNodeWithField embeddingNode = (UniversalEmbeddingNodeWithField) row
            .get(nodeIds[i]);
        if (embeddingNode != null) {
          int fieldId = ((EmbeddingOrGradWithField) grads[i]).getFieldId();
          UniversalEmbeddingNode embedding = embeddingNode.getFieldId2Emb().get(fieldId);
          if (embedding == null) {
            continue;
          }
          //0-sgd, 1-momentum, 2-adam, 3-adadelta
          if (optimType == 0) {
            sgdUpdate(embedding.getEmbeddings(), ((EmbeddingOrGradWithField) grads[i]).getValues(),
                floats, ints);
          } else if (optimType == 1) {
            momentumUpdate(embedding.getEmbeddings(), ((EmbeddingOrGradWithField) grads[i]).getValues(),
                embedding.getSlotsValues(), floats, ints);
          } else if (optimType == 2) {
            adamUpdate(embedding.getEmbeddings(), ((EmbeddingOrGradWithField) grads[i]).getValues(),
                embedding.getSlotsValues(), floats, ints);
          } else if (optimType == 3) {
            adaDeltaUpdate(embedding.getEmbeddings(), ((EmbeddingOrGradWithField) grads[i]).getValues(),
                embedding.getSlotsValues(), floats, ints);
          } else {
            throw new InvalidParameterException("Unsupported optimType " + optimType);
          }

          // update timestamp
          embeddingNode.getFieldId2UpdateTimestampSecond().put(fieldId, System.currentTimeMillis() / 1000);
        }
      }
    } finally {
      row.endWrite();
    }
  }
}

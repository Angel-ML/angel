package com.tencent.angel.spark.ml.ann_core.psf.getIVFHnswVectors;

import com.tencent.angel.graph.utils.GraphMatrixUtils;
import com.tencent.angel.ml.matrix.psf.get.base.GeneralPartGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.psagent.matrix.transport.router.KeyPart;
import com.tencent.angel.psagent.matrix.transport.router.operator.ILongKeyPartOp;
import com.tencent.angel.spark.ml.ann_core.IVFHnswVector;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import java.util.List;
import scala.Tuple2;

public class GetIVFHnswVectors extends GetFunc {

  private int matrixId;

  public GetIVFHnswVectors(GetIVFHnswVectorsParam param) {
    super(param);
    matrixId = param.getMatrixId();
  }

  public GetIVFHnswVectors(int matrixId, long[] nodes) {
    super(new GetIVFHnswVectorsParam(matrixId, nodes));
  }

  public GetIVFHnswVectors() {
    super(null);
  }

  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    GeneralPartGetParam param = (GeneralPartGetParam) partParam;
    KeyPart keyPart = param.getIndicesPart();
    long[] nodeIds = ((ILongKeyPartOp) keyPart).getKeys();
    ServerLongAnyRow row = GraphMatrixUtils.getPSLongKeyRow(psContext, param);

    Long2ObjectOpenHashMap<Tuple2<long[], float[][]>> vec = new Long2ObjectOpenHashMap<>(nodeIds.length);

    for (int i = 0; i < nodeIds.length; i += 1) {
      if (row.exist(nodeIds[i])) {
        IVFHnswVector tv = (IVFHnswVector) (row.get(nodeIds[i]));
        vec.put(nodeIds[i], new Tuple2<>(tv.getLabel(), tv.getVector()));
      }
    }

    return new GetIVFHnswVectorsPartResult(vec);
  }

  @Override
  public GetIVFHnswVectorsResult merge(List<PartitionGetResult> partResults) {
    int size = 0;
    for (PartitionGetResult result : partResults) {
      size += ((GetIVFHnswVectorsPartResult) result).size();
    }

    Long2ObjectOpenHashMap<Tuple2<long[], float[][]>> vector = new Long2ObjectOpenHashMap<>(size);

    for (PartitionGetResult result : partResults) {
      vector.putAll(((GetIVFHnswVectorsPartResult) result).getVec());
    }
    return new GetIVFHnswVectorsResult(vector);
  }


}

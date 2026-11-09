package com.tencent.angel.graph.client.psf.get.getnodes;

import com.tencent.angel.graph.data.GraphNode;
import com.tencent.angel.graph.utils.GraphMatrixUtils;
import com.tencent.angel.ml.math2.VFactory;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ml.matrix.psf.get.getrow.GetRowResult;
import com.tencent.angel.ml.matrix.psf.get.indexed.IndexPartGetLongResult;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.ps.storage.vector.element.IElement;
import com.tencent.angel.psagent.matrix.ResponseType;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap.Entry;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import java.util.List;

public class GetNodes extends GetFunc {

  public GetNodes(int matrixId, int[] partitionIds) {
    this(new GetNodesParam(matrixId, partitionIds));
  }

  public GetNodes(GetNodesParam param) {
    super(param);
  }

  public GetNodes() {
    super(null);
  }

  @Override
  public PartitionGetResult partitionGet(PartitionGetParam param) {
    ServerLongAnyRow row = GraphMatrixUtils.getPSLongKeyRow(psContext, param);

    ObjectIterator<Entry<IElement>> it = row.iterator();
    LongArrayList nodes = new LongArrayList();

    Boolean isHash = ((PartGetNodesParam) param).getHash();
    long start = isHash ? 0 : param.getPartKey().getStartCol();
    while (it.hasNext()) {
      Long2ObjectMap.Entry entry = it.next();
      GraphNode node = (GraphNode) entry.getValue();
      if (node.getFeats() != null && node.getNeighbors() == null) {
        nodes.add(entry.getLongKey() + start);
      }
    }

    return new IndexPartGetLongResult(param.getPartKey(), nodes.toLongArray());
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    int size = 0;
    for (PartitionGetResult result : partResults) {
      if (result instanceof IndexPartGetLongResult) {
        size += ((IndexPartGetLongResult) result).getValues().length;
      }
    }

    long[] values = new long[size];
    int start = 0;
    for (PartitionGetResult result : partResults) {
      if (result instanceof IndexPartGetLongResult) {
        long[] vals = ((IndexPartGetLongResult) result).getValues();
        System.arraycopy(vals, 0, values, start, vals.length);
        start += vals.length;
      }
    }

    return new GetRowResult(ResponseType.SUCCESS,
        VFactory.denseLongVector(values));
  }
}

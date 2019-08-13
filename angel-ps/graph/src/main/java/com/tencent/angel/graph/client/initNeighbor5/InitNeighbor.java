package com.tencent.angel.graph.client.initNeighbor5;

import com.tencent.angel.graph.data.Node;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.LongArrays;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.util.Random;

/* Mini-batch version of pushing csr neighbors */
public class InitNeighbor extends UpdateFunc {

  public InitNeighbor(InitNeighborParam param) {
    super(param);
  }

  public InitNeighbor() {
    this(null);
  }

  @Override
  public void partitionUpdate(PartitionUpdateParam partParam) {
    InitNeighborPartParam param = (InitNeighborPartParam) partParam;
    ServerLongAnyRow row = (ServerLongAnyRow) (psContext.getMatrixStorageManager().getRow(param.getPartKey(), 0));

    ObjectIterator<Long2ObjectMap.Entry<long[]>> it = param.getNodesToNeighbors()
      .long2ObjectEntrySet().iterator();

    Random random = new Random(System.currentTimeMillis());

    row.startWrite();
    try {
      while (it.hasNext()) {
        Long2ObjectMap.Entry<long[]> entry = it.next();
        Node node = (Node) row.get(entry.getLongKey());
        if (node == null) {
          node = new Node();
          row.set(entry.getLongKey(), node);
        }
        long[] neighbor = entry.getValue();
        LongArrays.shuffle(neighbor, random);
        node.setNeighbors(neighbor);
      }
    } finally {
      row.endWrite();
    }

  }

}

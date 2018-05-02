package com.tencent.angel.ml.lda.psf;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.*;
import com.tencent.angel.ml.matrix.psf.get.multi.PartitionGetRowsParam;
import com.tencent.angel.ps.impl.MatrixStorageManager;
import com.tencent.angel.ps.impl.matrix.ServerDenseIntRow;
import com.tencent.angel.ps.impl.matrix.ServerRow;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;

public class GetColumnFunc extends GetFunc {

  private final static Log LOG = LogFactory.getLog(GetColumnFunc.class);

  public GetColumnFunc(GetParam param) {
    super(param);
  }

  public GetColumnFunc() {
    super(null);
  }

  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    if (partParam instanceof PartitionGetRowsParam) {
      PartitionGetRowsParam param = (PartitionGetRowsParam) partParam;
      PartitionKey pkey = param.getPartKey();
      pkey = psContext.getMatrixMetaManager().getMatrixMeta(pkey.getMatrixId())
              .getPartitionMeta(pkey.getPartitionId()).getPartitionKey();

      List<Integer> reqCols = param.getRowIndexes();
      int size = reqCols.size();

      MatrixStorageManager manager = psContext.getMatrixStorageManager();
      Map<Long, Int2IntOpenHashMap> cks = new HashMap();

      for (Integer column: reqCols)
        cks.put((long) column, new Int2IntOpenHashMap());

      int rowOffset = pkey.getStartRow();
      int rowLength = pkey.getEndRow();
      for (int r = rowOffset; r < rowLength; r ++) {
        ServerRow row = manager.getRow(pkey, r);

        if (row instanceof ServerDenseIntRow) {
          for (int i = 0; i < size; i ++) {
            Int2IntOpenHashMap map = cks.get((long) reqCols.get(i));
            int k = ((ServerDenseIntRow) row).getData().get(reqCols.get(i));
            if (k > 0)
              map.put(row.getRowId(), k);
          }
        }
      }

      return new PartColumnResult(cks);
    } else {
      return null;
    }
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    List<PartColumnResult> columnResults = new ArrayList<>(partResults.size());
    for (PartitionGetResult r: partResults) {
      if (r instanceof PartColumnResult)
        columnResults.add((PartColumnResult) r);
      else
        LOG.error("r should be PartColumnResult but it is " + r.getClass());
    }

    // calculate columns
    PartColumnResult first = columnResults.get(0);

    for (int i = 1; i < columnResults.size(); i ++) {
      first.merge(columnResults.get(i));
    }

    return new ColumnGetResult(first.cks);
  }
}

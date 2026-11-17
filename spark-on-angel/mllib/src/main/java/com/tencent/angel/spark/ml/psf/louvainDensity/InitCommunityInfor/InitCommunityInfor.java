package com.tencent.angel.spark.ml.psf.louvainDensity.InitCommunityInfor;

import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ps.storage.matrix.ServerMatrix;
import com.tencent.angel.ps.storage.partition.RowBasedPartition;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InitCommunityInfor extends UpdateFunc {

  private static final Logger LOG = LoggerFactory.getLogger(InitCommunityInfor.class);

  /**
   * Create a new UpdateParam
   */
  public InitCommunityInfor(InitCommunityInforParam param) {
    super(param);
  }

  public InitCommunityInfor() {
    this(null);
  }

  @Override
  public void partitionUpdate(PartitionUpdateParam partParam) {
    PartInitCommunityInforParam param = (PartInitCommunityInforParam) partParam;
    ServerMatrix matrix = psContext.getMatrixStorageManager().getMatrix(partParam.getMatrixId());
    RowBasedPartition part = (RowBasedPartition) matrix
        .getPartition(partParam.getPartKey().getPartitionId());
    ServerLongAnyRow row = (ServerLongAnyRow) part.getRow(0);

    ObjectIterator<Long2ObjectMap.Entry<CommunityInforElement>> iter = param.getCommunityId2Infor()
        .long2ObjectEntrySet().iterator();

    row.startWrite();
    try {
      while (iter.hasNext()) {
        Long2ObjectMap.Entry<CommunityInforElement> entry = iter.next();
        CommunityInforElement element = entry.getValue();
        if (element == null) {
          row.set(entry.getLongKey(), null);
        } else {
          row.set(entry.getLongKey(),
              new CommunityInforElement(element.getNumberSelfEdge(), element.getSize(),
                  element.getWeightDegree(), element.getSelfWeight()));
        }
      }
    } finally {
      row.endWrite();
    }
  }
}

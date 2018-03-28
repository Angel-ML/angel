package com.tencent.angel.ml.treemodels.gbdt.fp.psf;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.update.enhance.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.enhance.UpdateFunc;
import com.tencent.angel.ml.matrix.psf.update.enhance.UpdateParam;
import com.tencent.angel.ps.impl.matrix.*;
import com.tencent.angel.psagent.PSAgentContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Clear elements on Parameter-Server matrix
 */
public class ClearUpdate extends UpdateFunc {

  public ClearUpdate(UpdateParam param) {
    super(param);
  }

  public ClearUpdate() {
    this(null);
  }

  public static class ClearUpdateParam extends UpdateParam {
    /**
     * Instantiates a new Clear updater param.
     *
     * @param matrixId the matrix id
     * @param updateClock the update clock
     */
    public ClearUpdateParam(int matrixId, boolean updateClock) {
      super(matrixId, updateClock);
    }

    /**
     * Split list.
     *
     * @return the list
     */
    @Override
    public List<PartitionUpdateParam> split() {
      List<PartitionKey> partList =
              PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);
      int size = partList.size();
      List<PartitionUpdateParam> partParamList = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        partParamList.add(new ClearPartitionUpdateParam(matrixId, partList.get(i), updateClock));
      }
      return partParamList;
    }
  }

  /**
   * The partition updater parameter.
   */
  public static class ClearPartitionUpdateParam extends PartitionUpdateParam {
    /**
     * Creates new partition updater parameter.
     *
     * @param matrixId the matrix id
     * @param partKey the part key
     * @param updateClock the update clock
     */
    public ClearPartitionUpdateParam(int matrixId, PartitionKey partKey, boolean updateClock) {
      super(matrixId, partKey, updateClock);
    }

    /**
     * Creates a new partition updater parameter by default.
     */
    public ClearPartitionUpdateParam() {
      super();
    }
  }

  /**
   * Partition update.
   *
   * @param partParam the partition parameter
   */
  @Override
  public void partitionUpdate(PartitionUpdateParam partParam) {
    ServerPartition part =
        psContext.getMatrixStorageManager()
            .getPart(partParam.getMatrixId(), partParam.getPartKey().getPartitionId());

    if (part != null) {
      int startRow = part.getPartitionKey().getStartRow();
      int endRow = part.getPartitionKey().getEndRow();
      for (int i = startRow; i < endRow; i++) {
        ServerRow row = part.getRow(i);
        if (row == null) {
          continue;
        }
        clear(row);
      }
    }

  }

  private void clear(ServerRow row) {
    switch (row.getRowType()) {
      case T_DOUBLE_SPARSE:
        clear((ServerSparseDoubleRow) row);
        break;

      case T_DOUBLE_DENSE:
        clear((ServerDenseDoubleRow) row);
        break;

      case T_FLOAT_SPARSE:
        clear((ServerSparseFloatRow) row);
        break;

      case T_FLOAT_DENSE:
        clear((ServerDenseFloatRow) row);
        break;

      case T_INT_SPARSE:
        clear((ServerSparseIntRow) row);
        break;

      case T_INT_DENSE:
        clear((ServerDenseIntRow) row);
        break;

      default:
        break;
    }
  }

  private void clear(ServerSparseDoubleRow row) {
    try {
      row.getLock().writeLock().lock();
      row.getData().clear();
    } finally {
      row.getLock().writeLock().unlock();
    }
  }

  private void clear(ServerDenseDoubleRow row) {
    try {
      row.getLock().writeLock().lock();
      byte[] data = row.getDataArray();
      Arrays.fill(data, (byte)0);
    } finally {
      row.getLock().writeLock().unlock();
    }
  }

  private void clear(ServerSparseFloatRow row) {
    try {
      row.getLock().writeLock().lock();
      row.getData().clear();
    } finally {
      row.getLock().writeLock().unlock();
    }
  }

  private void clear(ServerDenseFloatRow row) {
    try {
      row.getLock().writeLock().lock();
      byte[] data = row.getDataArray();
      Arrays.fill(data, (byte) 0);
    } finally {
      row.getLock().writeLock().unlock();
    }
  }

  private void clear(ServerSparseIntRow row) {
    try {
      row.getLock().writeLock().lock();
      row.getData().clear();
    } finally {
      row.getLock().writeLock().unlock();
    }
  }

  private void clear(ServerDenseIntRow row) {
    try {
      row.getLock().writeLock().lock();
      byte[] data = row.getDataArray();
      Arrays.fill(data, (byte) 0);
    } finally {
      row.getLock().writeLock().unlock();
    }
  }
}

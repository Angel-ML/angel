package com.tencent.angel.ml.treemodels.sketch.psf;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ml.matrix.psf.update.enhance.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.enhance.UpdateFunc;
import com.tencent.angel.ml.matrix.psf.update.enhance.UpdateParam;
import com.tencent.angel.ml.treemodels.sketch.HeapQuantileSketch;
import com.tencent.angel.ml.utils.Maths;
import com.tencent.angel.ps.impl.matrix.ServerDenseFloatRow;
import com.tencent.angel.ps.impl.matrix.ServerPartition;
import com.tencent.angel.ps.impl.matrix.ServerRow;
import com.tencent.angel.psagent.PSAgentContext;
import io.netty.buffer.ByteBuf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.nio.ByteBuffer;
import java.nio.FloatBuffer;
import java.util.ArrayList;
import java.util.List;

public class QSketchesMergeFunc extends UpdateFunc {
  private static final Log LOG = LogFactory.getLog(QSketchesMergeFunc.class);

  public QSketchesMergeFunc(QSketchesMergeParam param) {
    super(param);
  }

  public QSketchesMergeFunc(int matrixId, boolean updateClock, int[] rowIndexes,
                            int numWorker, int numQuantile,
                            HeapQuantileSketch[] sketches, long[] estimateNs) {
    super(new QSketchesMergeParam(matrixId, updateClock, rowIndexes,
            numWorker, numQuantile, sketches, estimateNs));
  }

  public QSketchesMergeFunc() {
    super(null);
  }

  public static class QSketchesMergeParam extends UpdateParam {
    protected int[] rowIndexes;
    protected int numWorker;
    protected int numQuantile;
    protected HeapQuantileSketch[] sketches;
    protected long[] estimateNs;

    public QSketchesMergeParam(int matrixId, boolean updateClock, int[] rowIndexes,
                               int numWorker, int numQuantile,
                               HeapQuantileSketch[] sketches, long[] estimateNs) {
      super(matrixId, updateClock);
      this.rowIndexes = rowIndexes;
      this.numWorker = numWorker;
      this.numQuantile = numQuantile;
      this.sketches = sketches;
      this.estimateNs = estimateNs;
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
        PartitionKey partKey = partList.get(i);
        List<Integer> partRowIndexes = new ArrayList<>();
        List<HeapQuantileSketch> partSketches = new ArrayList<>();
        List<Long> partEstimateNs = new ArrayList<>();
        for (int j = 0; j < rowIndexes.length; j++) {
          if (partKey.getStartRow() <= rowIndexes[j] && partKey.getEndRow() > rowIndexes[j]) {
            partRowIndexes.add(rowIndexes[j]);
            partSketches.add(sketches[j]);
            partEstimateNs.add(estimateNs[j]);
          }
        }
        if (partRowIndexes.size() > 0) {
          partParamList.add(new QSketchesPartitionMergeParam(matrixId, partKey, updateClock,
                  Maths.intList2Arr(partRowIndexes), numWorker, numQuantile,
                  partSketches.toArray(new HeapQuantileSketch[partSketches.size()]),
                  Maths.longList2Arr(partEstimateNs)));
        }
      }
      return partParamList;
    }

  }

  public static class QSketchesPartitionMergeParam extends PartitionUpdateParam {
    protected int[] rowIndexes;
    protected int numWorker;
    protected int numQuantile;
    protected HeapQuantileSketch[] sketches;
    protected long[] estimateNs;

    public QSketchesPartitionMergeParam(int matrixId, PartitionKey partKey, boolean updateClock,
                                        int[] rowIndexes, int numWorker, int numQuantile,
                                        HeapQuantileSketch[] sketches, long[] estimateNs) {
      super(matrixId, partKey, updateClock);
      this.rowIndexes = rowIndexes;
      this.numWorker = numWorker;
      this.numQuantile = numQuantile;
      this.sketches = sketches;
      this.estimateNs = estimateNs;
    }

    public QSketchesPartitionMergeParam() {
      super();
      this.rowIndexes = null;
      this.numWorker = -1;
      this.numQuantile = -1;
      this.sketches = null;
      this.estimateNs = null;
    }

    @Override
    public void serialize(ByteBuf buf) {
      super.serialize(buf);
      int size = rowIndexes.length;
      buf.writeInt(size);
      for (int i = 0; i < size; i++) {
        buf.writeInt(rowIndexes[i]);
        sketches[i].serialize(buf);
        buf.writeLong(estimateNs[i]);
      }
      buf.writeInt(numWorker);
      buf.writeInt(numQuantile);
    }

    @Override
    public void deserialize(ByteBuf buf) {
      super.deserialize(buf);
      int size = buf.readInt();
      rowIndexes = new int[size];
      sketches = new HeapQuantileSketch[size];
      estimateNs = new long[size];
      for (int i = 0; i < size; i++) {
        rowIndexes[i] = buf.readInt();
        sketches[i] = new HeapQuantileSketch(buf);
        estimateNs[i] = buf.readLong();
      }
      numWorker = buf.readInt();
      numQuantile = buf.readInt();
    }

    @Override
    public int bufferLen() {
      int res = super.bufferLen() + 12 + rowIndexes.length * 12;
      for (HeapQuantileSketch sketch: sketches) {
        res += sketch.bufferLen();
      }
      return res;
    }
  }

  /**
   * Partition update.
   *
   * @param partParam the partition parameter
   */
  @Override
  public void partitionUpdate(PartitionUpdateParam partParam) {
    ServerPartition part = psContext.getMatrixStorageManager().
        getPart(partParam.getMatrixId(), partParam.getPartKey().getPartitionId());

    if (part != null) {
      QSketchesPartitionMergeParam param = (QSketchesPartitionMergeParam) partParam;
      int startRow = part.getPartitionKey().getStartRow();
      int endRow = part.getPartitionKey().getEndRow();
      for (int i = 0; i < param.rowIndexes.length; i++) {
        if (startRow <= param.rowIndexes[i] && endRow > param.rowIndexes[i]) {
          ServerRow row = part.getRow(param.rowIndexes[i]);
          if (row == null) {
            throw new AngelException("Get null row: " + param.rowIndexes[i]);
          }
          switch (row.getRowType()) {
            case T_FLOAT_DENSE:
              qsketchMerge((ServerDenseFloatRow) row, param, i);
              break;

            default:
              break;
          }
        }
      }
    }
  }

  private void qsketchMerge(ServerDenseFloatRow row, QSketchesPartitionMergeParam partParam, int index) {
    try {
      row.getLock().writeLock().lock();
      // read sketch from row data, merge sketch
      HeapQuantileSketch qs1 = null;
      HeapQuantileSketch qs2 = partParam.sketches[index];
      byte[] data = row.getDataArray();
      ByteBuffer buf = ByteBuffer.wrap(data);
      buf.mark();
      int numMerged = buf.getInt();
      if (numMerged == partParam.numWorker) numMerged = 0;
      if (numMerged == 0)
        qs1 = new HeapQuantileSketch(qs2.getK(), partParam.estimateNs[index]);
      else
        qs1 = new HeapQuantileSketch(buf);
      LOG.debug(String.format("Row[%d] before merge[%d]: k[%d, %d], n[%d, %d], estimateN[%d, %d]",
              row.getRowId(), numMerged, qs1.getK(), qs2.getK(), qs1.getN(), qs2.getN(),
              qs1.getEstimateN(), qs2.getEstimateN()));
      qs1.merge(qs2);
      numMerged++;
      LOG.debug(String.format("Row[%d] after merge[%d]: k[%d, %d], n[%d, %d], estimateN[%d, %d]",
              row.getRowId(), numMerged, qs1.getK(), qs2.getK(), qs1.getN(), qs2.getN(),
              qs1.getEstimateN(), qs2.getEstimateN()));
      if (numMerged < partParam.numWorker) {
        // write sketch back to row data
        buf.reset();
        buf.putInt(numMerged);
        qs1.serialize(buf);
      }
      else {
        // get quantiles and write to row data
        float[] quantiles = qs1.getQuantiles(partParam.numQuantile);
        //LOG.info("Row[" + row.getRowId() + "] quantiles: " + Arrays.toString(quantiles));
        FloatBuffer writeBuf = row.getData();
        writeBuf.mark();
        writeBuf.put(Float.intBitsToFloat(numMerged));
        writeBuf.put(quantiles);
        writeBuf.reset();
      }
    } finally {
      row.getLock().writeLock().unlock();
    }
  }
}

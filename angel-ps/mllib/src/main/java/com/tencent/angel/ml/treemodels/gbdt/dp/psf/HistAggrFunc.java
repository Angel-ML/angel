package com.tencent.angel.ml.treemodels.gbdt.dp.psf;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ml.math.vector.DenseFloatVector;
import com.tencent.angel.ml.matrix.psf.update.enhance.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.enhance.UpdateFunc;
import com.tencent.angel.ml.matrix.psf.update.enhance.UpdateParam;
import com.tencent.angel.ps.impl.matrix.ServerDenseFloatRow;
import com.tencent.angel.ps.impl.matrix.ServerPartition;
import com.tencent.angel.ps.impl.matrix.ServerRow;
import com.tencent.angel.psagent.PSAgentContext;
import io.netty.buffer.ByteBuf;

import java.nio.FloatBuffer;
import java.util.ArrayList;
import java.util.List;

public class HistAggrFunc extends UpdateFunc {

  public HistAggrFunc(int matrixId, boolean updateClock, int rowId,
                       float[] array, int bitsPerItem) {
    super(new HistAggrParam(matrixId, updateClock, rowId, array, bitsPerItem));
  }

  public HistAggrFunc(int matrixId, boolean updateClock, int rowId,
                      DenseFloatVector vec, int bitsPerItem) {
    super(new HistAggrParam(matrixId, updateClock, rowId, vec.getValues(), bitsPerItem));
  }

  public HistAggrFunc() {
    super(null);
  }


  public static class HistAggrParam extends UpdateParam {
    private final int rowId;
    private final float[] array;
    private final int bitsPerItem;

    public HistAggrParam(int matrixId, boolean updateClock,
                         int rowId, float[] array, int bitsPerItem) {
      super(matrixId, updateClock);
      this.rowId = rowId;
      this.array = array;
      this.bitsPerItem = bitsPerItem;
    }

    @Override
    public List<PartitionUpdateParam> split() {
      List<PartitionKey> partList = PSAgentContext.get()
          .getMatrixMetaManager().getPartitions(matrixId, rowId);

      int size = partList.size();
      List<PartitionUpdateParam> partParams = new ArrayList<>(size);
      for (PartitionKey partKey : partList) {
        if (rowId < partKey.getStartRow() || rowId >= partKey.getEndRow()) {
          throw new AngelException("Wrong rowId");
        }
        partParams.add(new HistAggrPartitionParam(matrixId, partKey, updateClock,
          rowId, array, (int) partKey.getStartCol(), (int) partKey.getEndCol(), bitsPerItem));
      }
      return partParams;
    }
  }

  public static class HistAggrPartitionParam extends PartitionUpdateParam {
    private int rowId;
    private float[] array;
    private float[] slice;
    private int start;
    private int end;
    private int bitsPerItem;

    public HistAggrPartitionParam(int matrixId, PartitionKey partKey, boolean updateClock,
                                  int rowId, float[] array, int start, int end, int bitsPerItem) {
      super(matrixId, partKey, updateClock);
      this.rowId = rowId;
      this.array = array;
      this.start = start;
      this.end = end;
      this.bitsPerItem = bitsPerItem;
    }

    public HistAggrPartitionParam() {
      super();
    }

    @Override
    public void serialize(ByteBuf buf) {
      super.serialize(buf);
      buf.writeInt(rowId);
      buf.writeInt(end - start);
      buf.writeInt(bitsPerItem);
      // find the max abs
      double maxAbs = 0.0f;
      for (int i = start; i < end; i++) {
        maxAbs = Math.max(maxAbs, (double) Math.abs(array[i]));
      }
      buf.writeDouble(maxAbs);
      // compress data
      long maxPoint = (long) Math.pow(2, bitsPerItem - 1) - 1;
      for (int i = start; i < end; i++) {
        double value = array[i];
        long point = (long) Math.floor(Math.abs(value) / maxAbs * maxPoint);
        if (value > 1e-10 && point < Integer.MAX_VALUE) {
          point += (point < maxPoint && Math.random() > 0.5) ? 1 : 0;  // add Bernoulli random variable
        }
        byte[] tmp = long2Byte(point, bitsPerItem / 8, value < -1e-10);
        buf.writeBytes(tmp);
      }
    }

    @Override
    public void deserialize(ByteBuf buf) {
      super.deserialize(buf);
      rowId = buf.readInt();
      int length = buf.readInt();
      bitsPerItem = buf.readInt();
      double maxAbs = buf.readDouble();
      long maxPoint = (long) Math.pow(2, bitsPerItem - 1) - 1;
      byte[] itemBytes = new byte[bitsPerItem / 8];
      slice = new float[length];
      for (int i = 0; i < length; i++) {
        buf.readBytes(itemBytes);
        long point = byte2long(itemBytes);
        double parsedValue = (double) point / (double) maxPoint * maxAbs;
        slice[i] = (float) parsedValue;
      }
    }

    @Override
    public int bufferLen() {
      return super.bufferLen() + 20 + (end - start) * bitsPerItem / 8;
    }

    public static byte[] long2Byte(long value, int size, boolean isNeg) {
      assert Math.pow(2, 8 * size - 1) > value;
      byte[] rec = new byte[size];
      for (int i = 0; i < size; i++) {
        rec[size - i - 1] = (byte) value;
        value >>>= 8;
      }
      if (isNeg) {
        rec[0] |= 0x80;
      }
      return rec;
    }

    public static long byte2long(byte[] buffer){
      long rec = 0;
      boolean isNegative = (buffer[0] & 0x80) == 0x80;
      buffer[0] &= 0x7F;  // set the negative flag to 0

      int base = 0;
      for (int i = buffer.length - 1; i >= 0; i--) {
        long value = buffer[i] & 0x0FF;
        rec += value << base;
        base += 8;
      }

      if (isNegative) {
        rec = -1 * rec;
      }

      return rec;
    }

    public int getRowId() {
      return rowId;
    }

    public float[] getSlice() {
      return slice;
    }
  }

  @Override
  public void partitionUpdate(PartitionUpdateParam partParam) {
    ServerPartition part =
        psContext.getMatrixStorageManager()
            .getPart(partParam.getMatrixId(), partParam.getPartKey().getPartitionId());

    if (part != null) {
      HistAggrPartitionParam param = (HistAggrPartitionParam) partParam;
      ServerRow row = part.getRow(param.getRowId());
      if (row != null) {
        switch (row.getRowType()) {
          case T_FLOAT_DENSE:
            doUpdate((ServerDenseFloatRow) row, param.getSlice());
            break;
          default:
            throw new AngelException("Histogram aggregation only support DenseFloatRow");
        }
      }
    }
  }

  private void doUpdate(ServerDenseFloatRow row, float[] slice) {
    try {
      row.getLock().writeLock().lock();
      FloatBuffer data = row.getData();
      int size = row.size();
      for (int i = 0; i < size; i++) {
        data.put(i, data.get(i) + slice[i]);
      }
    } finally {
      row.getLock().writeLock().unlock();
    }
  }
}

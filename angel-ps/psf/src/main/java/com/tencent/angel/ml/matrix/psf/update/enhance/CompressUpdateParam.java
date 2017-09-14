package com.tencent.angel.ml.matrix.psf.update.enhance;


import com.tencent.angel.PartitionKey;
import com.tencent.angel.psagent.PSAgentContext;
import io.netty.buffer.ByteBuf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;


public class CompressUpdateParam extends UpdateParam {

  private static final Log LOG = LogFactory.getLog(CompressUpdateParam.class);

  private final int rowId;
  private final double[] array;
  private int bitsPerItem;

  public CompressUpdateParam(int matrixId, int rowId, double[] array, int bitsPerItem) {
    super(matrixId, false);
    this.rowId = rowId;
    this.array = array;
    this.bitsPerItem = bitsPerItem;
  }

  @Override
  public List<PartitionUpdateParam> split() {
    List<PartitionKey> partList = PSAgentContext.get()
        .getMatrixPartitionRouter()
        .getPartitionKeyList(matrixId, rowId);

    int size = partList.size();
    List<PartitionUpdateParam> partParams = new ArrayList<PartitionUpdateParam>(size);
    for (PartitionKey part : partList) {
      if (rowId < part.getStartRow() || rowId >= part.getEndRow()) {
        throw new RuntimeException("Wrong rowId!");
      }
      partParams.add(new CompressPartitionUpdateParam(matrixId, part, rowId,
          (int) part.getStartCol(), (int) part.getEndCol(), array, bitsPerItem));
    }

    return partParams;
  }

  public static class CompressPartitionUpdateParam extends PartitionUpdateParam{

    private static final Log LOG = LogFactory.getLog(CompressPartitionUpdateParam.class);

    private int rowId;
    private int start;
    private int end;
    private double[] array;
    private double[] arraySlice;
    int bitPerItem;

    public CompressPartitionUpdateParam(
        int matrixId, PartitionKey partKey, int rowId, int start, int end, double[] array, int bitPerItem) {
      super(matrixId, partKey, false);
      this.rowId = rowId;
      this.start = start;
      this.end = end;
      this.array = array;
      this.bitPerItem = bitPerItem;
    }

    public CompressPartitionUpdateParam() {
      super();
    }

    @Override
    public void serialize(ByteBuf buf) {
      super.serialize(buf);
      buf.writeInt(rowId);
      buf.writeInt(end - start);
      buf.writeInt(bitPerItem);
      // find the max abs
      double maxAbs = 0.0;
      for (int i = start; i < end; i++) {
        maxAbs = Math.abs(array[i]) > maxAbs ? Math.abs(array[i]): maxAbs;
      }
      buf.writeDouble(maxAbs);
      // compress data
      long startTime = System.currentTimeMillis();
      int byteSum = 0;
      int maxPoint = (int) Math.pow(2, bitPerItem - 1) - 1;
      for (int i = start; i < end; i++) {
        double value = array[i];
        int point = (int) Math.floor(Math.abs(value) / maxAbs * maxPoint);
        point += (point < maxPoint && Math.random() > 0.5) ? 1 : 0; // add Bernoulli random variable
        byte[] tmp = int2Byte(point, bitPerItem / 8);
        if (value < -1e-10) {
          tmp[0] |= 0x80;
        }
        buf.writeBytes(tmp);
        byteSum += bitPerItem / 8;
      }
      LOG.debug(String.format("Compress %d doubles from %d bytes to %d bytes, " +
              "cost %d ms", end - start, (end - start) * 8, byteSum + 12,
          System.currentTimeMillis() - startTime));
    }

    @Override
    public void deserialize(ByteBuf buf) {
      super.deserialize(buf);
      rowId = buf.readInt();
      int length = buf.readInt();
      int bitPerItem = buf.readInt();
      double maxAbs = buf.readDouble();
      int maxPoint = (int) Math.pow(2, bitPerItem - 1) - 1;

      byte[] itemBytes = new byte[bitPerItem / 8];
      arraySlice = new double[length];
      for (int i = 0; i < length; i++) {
        buf.readBytes(itemBytes);
        int point = byte2int(itemBytes);
        double parsedValue = (double) point / (double) maxPoint * maxAbs;
        arraySlice[i] = parsedValue;
      }
      LOG.debug(String.format("parse compressed %d double data, max abs: %f, max point: %d",
          length, maxAbs, maxPoint));
    }

    @Override
    public int bufferLen() {
      return super.bufferLen() + 20 + (end - start) * bitPerItem / 8;
    }

    public int getRowId() {
      return rowId;
    }

    public double[] getArraySlice() {
      return arraySlice;
    }

    @Override
    public String toString() {
      return "CompressPartitionUpdateParam [rowId=" + rowId + ", bitPerItem=" +  bitPerItem + ", toString()="
          + super.toString() + "]";
    }

    private static byte[] int2Byte(int value, int size) {
      assert Math.pow(2, 8 * size - 1) > value;
      byte[] rec = new byte[size];
      for (int i = 0; i < size; i++) {
        rec[size - i - 1] = (byte) value;
        value >>>= 8;
      }
      return rec;
    }

    private static int byte2int(byte[] buffer){
      int rec = 0;
      boolean isNegative = (buffer[0] & 0x80) == 0x80;
      buffer[0] &= 0x7F;  // set the negative flag to 0

      int base = 0;
      for (int i = buffer.length - 1; i >= 0; i--) {
        byte value = buffer[i];
        value <<= base;
        base += 8;
        rec += value;
      }

      if (isNegative) {
        rec = ~rec;
      }

      return rec;
    }
  }


}

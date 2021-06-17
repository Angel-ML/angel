package com.tencent.angel.common;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.math2.VFactory;
import com.tencent.angel.ml.math2.storage.IntDoubleSparseVectorStorage;
import com.tencent.angel.ml.math2.storage.IntDoubleVectorStorage;
import com.tencent.angel.ml.math2.storage.IntFloatSparseVectorStorage;
import com.tencent.angel.ml.math2.storage.IntFloatVectorStorage;
import com.tencent.angel.ml.math2.storage.LongFloatSparseVectorStorage;
import com.tencent.angel.ml.math2.storage.LongFloatVectorStorage;
import com.tencent.angel.ml.math2.storage.LongIntSparseVectorStorage;
import com.tencent.angel.ml.math2.storage.LongIntVectorStorage;
import com.tencent.angel.ml.math2.vector.IntDoubleVector;
import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ml.math2.vector.LongFloatVector;
import com.tencent.angel.ml.math2.vector.LongIntVector;
import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ps.server.data.request.KeyType;
import com.tencent.angel.ps.server.data.request.ValueType;
import com.tencent.angel.ps.storage.vector.ServerRow;
import com.tencent.angel.ps.storage.vector.ServerRowFactory;
import com.tencent.angel.ps.storage.vector.element.IElement;
import com.tencent.angel.psagent.matrix.transport.router.CompStreamKeyValuePart;
import com.tencent.angel.psagent.matrix.transport.router.DataPartFactory;
import com.tencent.angel.psagent.matrix.transport.router.KeyPart;
import com.tencent.angel.psagent.matrix.transport.router.KeyValuePart;
import com.tencent.angel.psagent.matrix.transport.router.RouterType;
import com.tencent.angel.psagent.matrix.transport.router.ValuePart;
import com.tencent.angel.utils.ByteBufUtils;
import com.tencent.angel.utils.StringUtils;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.ints.Int2DoubleMap.Entry;
import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2FloatMap;
import it.unimi.dsi.fastutil.ints.Int2FloatOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2FloatMap;
import it.unimi.dsi.fastutil.longs.Long2FloatOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import java.io.UnsupportedEncodingException;

public class ByteBufSerdeUtils {

  public static final int BOOLEN_LENGTH = 1;
  public static final int BYTE_LENGTH = 1;
  public static final int CHAR_LENGTH = 2;
  public static final int INT_LENGTH = 4;
  public static final int LONG_LENGTH = 8;
  public static final int FLOAT_LENGTH = 4;
  public static final int DOUBLE_LENGTH = 8;

  public static final float[] emptyFloats = new float[0];

  // =======================================================
  // Boolean
  public static void serializeBoolean(ByteBuf out, boolean value) {
    out.writeBoolean(value);
  }

  public static boolean deserializeBoolean(ByteBuf in) {
    return in.readBoolean();
  }

  public static int serializedBooleanLen(boolean value) {
    return BOOLEN_LENGTH;
  }

  // =======================================================
  // Byte
  public static void serializeByte(ByteBuf out, byte value) {
    out.writeByte(value);
  }

  public static byte deserializeByte(ByteBuf in) {
    return in.readByte();
  }

  public static int serializedByteLen(byte value) {
    return BYTE_LENGTH;
  }

  // =======================================================
  // Int
  public static void serializeChar(ByteBuf out, int value) {
    out.writeChar(value);
  }

  public static char deserializeChar(ByteBuf in) {
    return in.readChar();
  }

  public static int serializedCharLen(char value) {
    return CHAR_LENGTH;
  }

  // =======================================================
  // Int
  public static void serializeInt(ByteBuf out, int value) {
    out.writeInt(value);
  }

  public static int deserializeInt(ByteBuf in) {
    return in.readInt();
  }

  public static int serializedIntLen(int value) {
    return INT_LENGTH;
  }

  // =======================================================
  // Long
  public static void serializeLong(ByteBuf out, long value) {
    out.writeLong(value);
  }

  public static long deserializeLong(ByteBuf in) {
    return in.readLong();
  }

  public static int serializedLongLen(long value) {
    return LONG_LENGTH;
  }

  // =======================================================
  // Float
  public static void serializeFloat(ByteBuf out, float value) {
    out.writeFloat(value);
  }

  public static float deserializeFloat(ByteBuf in) {
    return in.readFloat();
  }

  public static int serializedFloatLen(float value) {
    return FLOAT_LENGTH;
  }

  // =======================================================
  // String
  public static void serializeUTF8(ByteBuf out, String value) {
    try {
      byte[] strBytes = value.getBytes("UTF-8");
      serializeBytes(out, strBytes);
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(
          "Serialize " + value + " failed, details = " + StringUtils.stringifyException(e));
    }
  }

  public static String deserializeUTF8(ByteBuf in) {
    try {
      return new String(deserializeBytes(in), "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(
          "Deserialize string failed, details = " + StringUtils.stringifyException(e));
    }
  }

  public static int serializedUTF8Len(String value) {
    try {
      return serializedBytesLen(value.getBytes("UTF-8"));
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(
          "Calculate length of " + value + " failed, details = " + StringUtils
              .stringifyException(e));
    }
  }

  // =======================================================
  // Double
  public static void serializeDouble(ByteBuf out, double value) {
    out.writeDouble(value);
  }

  public static double deserializeDouble(ByteBuf in) {
    return in.readDouble();
  }

  public static int serializedDoubleLen(double value) {
    return DOUBLE_LENGTH;
  }

  // =======================================================
  // Byte array
  public static void serializeBytes(ByteBuf out, byte[] values, int start, int end) {
    serializeInt(out, end - start);
    out.writeBytes(values, start, end - start);
  }

  public static void serializeBytes(ByteBuf out, byte[] values) {
    serializeBytes(out, values, 0, values.length);
  }

  public static byte[] deserializeBytes(ByteBuf in) {
    int len = in.readInt();
    byte[] values = new byte[len];
    in.readBytes(values);
    return values;
  }

  public static int serializedBytesLen(byte[] values, int start, int end) {
    return BYTE_LENGTH + (end - start) * BYTE_LENGTH;
  }

  public static int serializedBytesLen(byte[] values) {
    return serializedBytesLen(values, 0, values.length);
  }

  // =======================================================
  // Int array
  public static void serializeInts(ByteBuf out, int[] values, int start, int end) {
    serializeInt(out, end - start);
    for (int i = start; i < end; i++) {
      serializeInt(out, values[i]);
    }
  }

  public static void serializeInts(ByteBuf out, int[] values) {
    serializeInts(out, values, 0, values.length);
  }

  public static int[] deserializeInts(ByteBuf in) {
    int len = in.readInt();
    int[] values = new int[len];
    for (int i = 0; i < len; i++) {
      values[i] = deserializeInt(in);
    }
    return values;
  }

  public static int serializedIntsLen(int[] values, int start, int end) {
    return INT_LENGTH + (end - start) * INT_LENGTH;
  }

  public static int serializedIntsLen(int[] values) {
    return serializedIntsLen(values, 0, values.length);
  }

  // =======================================================
  // Long array
  public static void serializeLongs(ByteBuf out, long[] values, int start, int end) {
    serializeInt(out, end - start);
    for (int i = start; i < end; i++) {
      serializeLong(out, values[i]);
    }
  }

  public static void serializeLongs(ByteBuf out, long[] values) {
    serializeLongs(out, values, 0, values.length);
  }

  public static long[] deserializeLongs(ByteBuf in) {
    int len = in.readInt();
    long[] values = new long[len];
    for (int i = 0; i < len; i++) {
      values[i] = deserializeLong(in);
    }
    return values;
  }

  public static int serializedLongsLen(long[] values, int start, int end) {
    return LONG_LENGTH + (end - start) * LONG_LENGTH;
  }

  public static int serializedLongsLen(long[] values) {
    return serializedLongsLen(values, 0, values.length);
  }

  // =======================================================
  // Float array
  public static void serializeFloats(ByteBuf out, float[] values, int start, int end) {
    serializeInt(out, end - start);
    for (int i = start; i < end; i++) {
      serializeFloat(out, values[i]);
    }
  }

  public static void serializeFloats(ByteBuf out, float[] values) {
    serializeFloats(out, values, 0, values.length);
  }

  public static float[] deserializeFloats(ByteBuf in) {
    int len = in.readInt();
    float[] values = new float[len];
    for (int i = 0; i < len; i++) {
      values[i] = deserializeFloat(in);
    }
    return values;
  }

  public static int serializedFloatsLen(float[] values, int start, int end) {
    return INT_LENGTH + (end - start) * FLOAT_LENGTH;
  }

  public static int serializedFloatsLen(float[] values) {
    return serializedFloatsLen(values, 0, values.length);
  }

  // =======================================================
  // Double array
  public static void serializeDoubles(ByteBuf out, double[] values, int start, int end) {
    serializeInt(out, end - start);
    for (int i = start; i < end; i++) {
      serializeDouble(out, values[i]);
    }
  }

  public static void serializeDoubles(ByteBuf out, double[] values) {
    serializeDoubles(out, values, 0, values.length);
  }

  public static double[] deserializeDoubles(ByteBuf in) {
    int len = in.readInt();
    double[] values = new double[len];
    for (int i = 0; i < len; i++) {
      values[i] = deserializeDouble(in);
    }
    return values;
  }

  public static int serializedDoublesLen(double[] values, int start, int end) {
    return DOUBLE_LENGTH + (end - start) * DOUBLE_LENGTH;
  }

  public static int serializedDoublesLen(double[] values) {
    return serializedDoublesLen(values, 0, values.length);
  }

  // =======================================================
  // String array
  public static void serializeUTF8s(ByteBuf out, String[] values) {
    serializeInt(out, values.length);
    for (int i = 0; i < values.length; i++) {
      if (values[i] != null) {
        serializeBoolean(out, true);
        serializeUTF8(out, values[i]);
      } else {
        serializeBoolean(out, false);
      }
    }
  }

  public static String[] deserializeUTF8s(ByteBuf in) {
    int len = deserializeInt(in);
    String[] values = new String[len];
    for (int i = 0; i < len; i++) {
      if (deserializeBoolean(in)) {
        values[i] = deserializeUTF8(in);
      }
    }
    return values;
  }

  public static int serializedUTF8sLen(String[] values) {
    int len = INT_LENGTH;
    for (int i = 0; i < len; i++) {
      len += BOOLEN_LENGTH;
      if (values[i] != null) {
        len += serializedUTF8Len(values[i]);
      }
    }
    return len;
  }

  // =======================================================
  // 2-D Int array
  public static void serialize2DInts(ByteBuf out, int[][] values, int start, int end) {
    serializeInt(out, end - start);
    for (int i = start; i < end; i++) {
      serializeInts(out, values[i]);
    }
  }

  public static void serialize2DInts(ByteBuf out, int[][] values) {
    serialize2DInts(out, values, 0, values.length);
  }

  public static int[][] deserialize2DInts(ByteBuf in) {
    int len = in.readInt();
    int[][] values = new int[len][];
    for (int i = 0; i < len; i++) {
      values[i] = deserializeInts(in);
    }
    return values;
  }

  public static int serialized2DIntsLen(int[][] values, int start, int end) {
    int len = INT_LENGTH;
    for (int i = start; i < end; i++) {
      len += serializedIntsLen(values[i]);
    }
    return len;
  }

  public static int serialized2DIntsLen(int[][] values) {
    return serialized2DIntsLen(values, 0, values.length);
  }

  // =======================================================
  // 2-D Long array
  public static void serialize2DLongs(ByteBuf out, long[][] values, int start, int end) {
    serializeInt(out, end - start);
    for (int i = start; i < end; i++) {
      serializeLongs(out, values[i]);
    }
  }

  public static void serialize2DLongs(ByteBuf out, long[][] values) {
    serialize2DLongs(out, values, 0, values.length);
  }

  public static long[][] deserialize2DLongs(ByteBuf in) {
    int len = in.readInt();
    long[][] values = new long[len][];
    for (int i = 0; i < len; i++) {
      values[i] = deserializeLongs(in);
    }
    return values;
  }

  public static int serialized2DLongsLen(long[][] values, int start, int end) {
    int len = INT_LENGTH;
    for (int i = start; i < end; i++) {
      len += serializedLongsLen(values[i]);
    }
    return len;
  }

  public static int serialized2DLongsLen(long[][] values) {
    return serialized2DLongsLen(values, 0, values.length);
  }

  // =======================================================
  // 2-D Float array
  public static void serialize2DFloats(ByteBuf out, float[][] values, int start, int end) {
    serializeInt(out, end - start);
    for (int i = start; i < end; i++) {
      serializeFloats(out, values[i]);
    }
  }

  public static void serialize2DFloats(ByteBuf out, float[][] values) {
    serialize2DFloats(out, values, 0, values.length);
  }

  public static float[][] deserialize2DFloats(ByteBuf in) {
    int len = in.readInt();
    float[][] values = new float[len][];
    for (int i = 0; i < len; i++) {
      values[i] = deserializeFloats(in);
    }
    return values;
  }

  public static int serialized2DFloatsLen(float[][] values, int start, int end) {
    int len = INT_LENGTH;
    for (int i = start; i < end; i++) {
      len += serializedFloatsLen(values[i]);
    }
    return len;
  }

  public static int serialized2DFloatsLen(float[][] values) {
    return serialized2DFloatsLen(values, 0, values.length);
  }

  // =======================================================
  // 2-D Double array
  public static void serialize2DDoubles(ByteBuf out, double[][] values, int start, int end) {
    serializeInt(out, end - start);
    for (int i = start; i < end; i++) {
      serializeDoubles(out, values[i]);
    }
  }

  public static void serialize2DDoubles(ByteBuf out, double[][] values) {
    serialize2DDoubles(out, values, 0, values.length);
  }

  public static double[][] deserialize2DDoubles(ByteBuf in) {
    int len = in.readInt();
    double[][] values = new double[len][];
    for (int i = 0; i < len; i++) {
      values[i] = deserializeDoubles(in);
    }
    return values;
  }

  public static int serialized2DDoublesLen(double[][] values, int start, int end) {
    int len = INT_LENGTH;
    for (int i = start; i < end; i++) {
      len += serializedDoublesLen(values[i]);
    }
    return len;
  }

  public static int serialized2DDoublesLen(double[][] values) {
    return serialized2DDoublesLen(values, 0, values.length);
  }

  // =======================================================
  // Vector
  public static final int DENSE_STORAGE_TYPE = 0;
  public static final int SPARSE_STORAGE_TYPE = 1;
  public static final int SORTED_STORAGE_TYPE = 2;

  public static void serializeVector(ByteBuf out, Vector vector) {
    // Meta data
    serializeInt(out, vector.getRowId());
    serializeLong(out, vector.dim());
    serializeInt(out, vector.getType().getNumber());

    switch (vector.getType()) {
      case T_DOUBLE_DENSE:
      case T_DOUBLE_SPARSE:
        serializeIntDoubleVector(out, (IntDoubleVector) vector);
        break;
      case T_FLOAT_DENSE:
      case T_FLOAT_SPARSE:
        serializeIntFloatVector(out, (IntFloatVector) vector);
        break;
      case T_FLOAT_SPARSE_LONGKEY:
        serializeLongFloatVector(out, (LongFloatVector) vector);
        break;
      case T_INT_SPARSE_LONGKEY:
        serializeLongIntVector(out, (LongIntVector) vector);
        break;
      //TODO: other vector type
      default:
        throw new UnsupportedOperationException("Unsupport vector type " + vector.getType());
    }
  }

  public static Vector deserializeVector(ByteBuf in) {
    int rowId = deserializeInt(in);
    long dim = deserializeLong(in);
    RowType type = RowType.valueOf(deserializeInt(in));

    Vector vector;

    switch (type) {
      case T_DOUBLE_DENSE:
      case T_DOUBLE_SPARSE:
        vector = deserializeIntDoubleVector(in, dim);
        break;
      case T_FLOAT_DENSE:
      case T_FLOAT_SPARSE:
        vector = deserializeIntFloatVector(in, dim);
        break;
      case T_FLOAT_SPARSE_LONGKEY:
        vector = deserializeLongFloatVector(in, dim);
        break;
      case T_INT_SPARSE_LONGKEY:
        vector = deserializeLongIntVector(in, dim);
        break;
      // TODO: other vector type
      default:
        throw new UnsupportedOperationException("Unsupport vector type " + type);
    }

    vector.setRowId(rowId);
    return vector;
  }

  public static int serializedVectorLen(Vector vector) {
    int len = serializedIntLen(vector.getRowId())
        + serializedLongLen(vector.dim())
        + serializedIntLen(vector.getType().getNumber());

    switch (vector.getType()) {
      case T_DOUBLE_DENSE:
      case T_DOUBLE_SPARSE:
        len += serializedIntDoubleVectorLen((IntDoubleVector) vector);
        break;
      case T_FLOAT_DENSE:
      case T_FLOAT_SPARSE:
        len += serializedIntFloatVectorLen((IntFloatVector) vector);
        break;
      case T_FLOAT_SPARSE_LONGKEY:
        len += serializedLongFloatVectorLen((LongFloatVector) vector);
        break;
      case T_INT_SPARSE_LONGKEY:
        len += serializedLongIntVectorLen((LongIntVector) vector);
        break;
      //TODO: other vector type
      default:
        throw new UnsupportedOperationException("Unsupport vector type " + vector.getType());
    }

    return len;
  }

  public static int serializedIntDoubleVectorLen(IntDoubleVector vector) {
    int len = 0;
    IntDoubleVectorStorage storage = vector.getStorage();
    if (storage.isDense()) {
      len += serializedIntLen(DENSE_STORAGE_TYPE);
      len += serializedDoublesLen(storage.getValues());
    } else if (storage.isSparse()) {
      len += serializedIntLen(SPARSE_STORAGE_TYPE);
      len += serializedIntLen(storage.size());
      len += storage.size() * (INT_LENGTH + DOUBLE_LENGTH);
    } else if (storage.isSorted()) {
      len += serializedIntLen(SORTED_STORAGE_TYPE);
      len += serializedIntsLen(vector.getStorage().getIndices());
      len += serializedDoublesLen(vector.getStorage().getValues());
    } else {
      throw new UnsupportedOperationException(
          "Unsupport storage type " + vector.getStorage().getClass());
    }

    return len;
  }

  public static int serializedIntFloatVectorLen(IntFloatVector vector) {
    int len = 0;
    IntFloatVectorStorage storage = vector.getStorage();
    if (storage.isDense()) {
      len += serializedIntLen(DENSE_STORAGE_TYPE);
      len += serializedFloatsLen(storage.getValues());
    } else if (storage.isSparse()) {
      len += serializedIntLen(SPARSE_STORAGE_TYPE);
      len += serializedIntLen(storage.size());
      len += storage.size() * (INT_LENGTH + FLOAT_LENGTH);
    } else if (storage.isSorted()) {
      len += serializedIntLen(SORTED_STORAGE_TYPE);
      len += serializedIntsLen(vector.getStorage().getIndices());
      len += serializedFloatsLen(vector.getStorage().getValues());
    } else {
      throw new UnsupportedOperationException(
          "Unsupport storage type " + vector.getStorage().getClass());
    }

    return len;
  }

  // IntDoubleVector
  private static void serializeIntDoubleVector(ByteBuf out, IntDoubleVector vector) {
    IntDoubleVectorStorage storage = vector.getStorage();
    if (storage.isDense()) {
      serializeInt(out, DENSE_STORAGE_TYPE);
      serializeDoubles(out, storage.getValues());
    } else if (storage.isSparse()) {
      serializeInt(out, SPARSE_STORAGE_TYPE);
      serializeInt(out, storage.size());
      ObjectIterator<Entry> iter = storage.entryIterator();
      while (iter.hasNext()) {
        Entry e = iter.next();
        serializeInt(out, e.getIntKey());
        serializeDouble(out, e.getDoubleValue());
      }
    } else if (storage.isSorted()) {
      serializeInt(out, SORTED_STORAGE_TYPE);
      int[] indices = vector.getStorage().getIndices();
      double[] values = vector.getStorage().getValues();
      serializeInts(out, indices);
      serializeDoubles(out, values);
    } else {
      throw new UnsupportedOperationException(
          "Unsupport storage type " + vector.getStorage().getClass());
    }
  }

  private static IntDoubleVector deserializeIntDoubleVector(ByteBuf in, long dim) {
    int storageType = deserializeInt(in);
    switch (storageType) {
      case DENSE_STORAGE_TYPE:
        return VFactory.denseDoubleVector(deserializeDoubles(in));

      case SPARSE_STORAGE_TYPE:
        int len = deserializeInt(in);
        Int2DoubleOpenHashMap idToValueMap = new Int2DoubleOpenHashMap(len);
        for (int i = 0; i < len; i++) {
          idToValueMap.put(deserializeInt(in), deserializeDouble(in));
        }
        return new IntDoubleVector((int) dim,
            new IntDoubleSparseVectorStorage((int) dim, idToValueMap));

      case SORTED_STORAGE_TYPE:
        return VFactory.sortedDoubleVector((int) dim, deserializeInts(in), deserializeDoubles(in));

      default:
        throw new UnsupportedOperationException("Unsupport storage type " + storageType);
    }
  }

  // IntFloatVector
  private static void serializeIntFloatVector(ByteBuf out, IntFloatVector vector) {
    IntFloatVectorStorage storage = vector.getStorage();
    if (storage.isDense()) {
      serializeInt(out, DENSE_STORAGE_TYPE);
      serializeFloats(out, storage.getValues());
    } else if (storage.isSparse()) {
      serializeInt(out, SPARSE_STORAGE_TYPE);
      serializeInt(out, storage.size());
      ObjectIterator<Int2FloatMap.Entry> iter = storage.entryIterator();
      while (iter.hasNext()) {
        Int2FloatMap.Entry e = iter.next();
        serializeInt(out, e.getIntKey());
        serializeFloat(out, e.getFloatValue());
      }
    } else if (storage.isSorted()) {
      serializeInt(out, SORTED_STORAGE_TYPE);
      int[] indices = vector.getStorage().getIndices();
      float[] values = vector.getStorage().getValues();
      serializeInts(out, indices);
      serializeFloats(out, values);
    } else {
      throw new UnsupportedOperationException(
          "Unsupport storage type " + vector.getStorage().getClass());
    }
  }

  private static IntFloatVector deserializeIntFloatVector(ByteBuf in, long dim) {
    int storageType = deserializeInt(in);
    switch (storageType) {
      case DENSE_STORAGE_TYPE:
        return VFactory.denseFloatVector(deserializeFloats(in));

      case SPARSE_STORAGE_TYPE:
        int len = deserializeInt(in);
        Int2FloatOpenHashMap idToValueMap = new Int2FloatOpenHashMap(len);
        for (int i = 0; i < len; i++) {
          idToValueMap.put(deserializeInt(in), deserializeFloat(in));
        }
        return new IntFloatVector((int) dim,
            new IntFloatSparseVectorStorage((int) dim, idToValueMap));

      case SORTED_STORAGE_TYPE:
        return VFactory.sortedFloatVector((int) dim, deserializeInts(in), deserializeFloats(in));

      default:
        throw new UnsupportedOperationException("Unsupport storage type " + storageType);
    }
  }

  // IntFloatVector array
  public static void serializeIntFloatVectors(ByteBuf out, IntFloatVector[] vectors) {
    int start = 0;
    int end = vectors.length;
    serializeInt(out, end - start);
    for (int i = start; i < end; i++) {
      IntFloatVector vector = vectors[i];
      serializeInt(out, vector.getRowId());
      serializeLong(out, vector.dim());
//      serializeInt(out, vector.getType().getNumber()); // no need to record type
      serializeIntFloatVector(out, vectors[i]);
    }
  }

  public static IntFloatVector[] deserializeIntFloatVectors(ByteBuf in) {
    int len = in.readInt();
    IntFloatVector[] values = new IntFloatVector[len];
    for (int i = 0; i < len; i++) {
      int rowId = deserializeInt(in);
      long dim = deserializeLong(in);
//      RowType type = RowType.valueOf(deserializeInt(in));
      values[i] = deserializeIntFloatVector(in, dim);
      values[i].setRowId(rowId);
    }
    return values;
  }

  public static int serializedIntFloatVectorsLen(IntFloatVector[] values) {
    int start = 0;
    int end = values.length;
    int len = 0;
    for (int i = start; i < end; i++) {
      len += serializedIntFloatVectorLen(values[i]);
    }
    return len;
  }

  public static int serializedLongFloatVectorLen(LongFloatVector vector) {
    int len = 0;
    LongFloatVectorStorage storage = vector.getStorage();
    if (storage.isSparse()) {
      len += serializedIntLen(SPARSE_STORAGE_TYPE);
      len += serializedIntLen(storage.size());
      len += storage.size() * (INT_LENGTH + FLOAT_LENGTH);
    } else if (storage.isSorted()) {
      len += serializedIntLen(SORTED_STORAGE_TYPE);
      len += serializedLongsLen(vector.getStorage().getIndices());
      len += serializedFloatsLen(vector.getStorage().getValues());
    } else {
      throw new UnsupportedOperationException(
          "Unsupport storage type " + vector.getStorage().getClass());
    }

    return len;
  }

  // LongFloatVector
  private static void serializeLongFloatVector(ByteBuf out, LongFloatVector vector) {
    LongFloatVectorStorage storage = vector.getStorage();
    if (storage.isSparse()) {
      serializeInt(out, SPARSE_STORAGE_TYPE);
      serializeInt(out, storage.size());
      ObjectIterator<Long2FloatMap.Entry> iter = storage.entryIterator();
      while (iter.hasNext()) {
        Long2FloatMap.Entry e = iter.next();
        serializeLong(out, e.getLongKey());
        serializeFloat(out, e.getFloatValue());
      }
    } else if (storage.isSorted()) {
      serializeInt(out, SORTED_STORAGE_TYPE);
      long[] indices = vector.getStorage().getIndices();
      float[] values = vector.getStorage().getValues();
      serializeLongs(out, indices);
      serializeFloats(out, values);
    } else {
      throw new UnsupportedOperationException(
          "Unsupport storage type " + vector.getStorage().getClass());
    }
  }

  private static LongFloatVector deserializeLongFloatVector(ByteBuf in, long dim) {
    int storageType = deserializeInt(in);
    switch (storageType) {

      case SPARSE_STORAGE_TYPE:
        int len = deserializeInt(in);
        Long2FloatOpenHashMap idToValueMap = new Long2FloatOpenHashMap(len);
        for (int i = 0; i < len; i++) {
          idToValueMap.put(deserializeInt(in), deserializeFloat(in));
        }
        return new LongFloatVector((int) dim,
            new LongFloatSparseVectorStorage((int) dim, idToValueMap));

      case SORTED_STORAGE_TYPE:
        return VFactory
            .sortedLongKeyFloatVector((int) dim, deserializeLongs(in), deserializeFloats(in));

      default:
        throw new UnsupportedOperationException("Unsupport storage type " + storageType);
    }
  }

  // LongFloatVector array
  public static void serializeLongFloatVectors(ByteBuf out, LongFloatVector[] vectors) {
    int start = 0;
    int end = vectors.length;
    serializeInt(out, end - start);
    for (int i = start; i < end; i++) {
      LongFloatVector vector = vectors[i];
      serializeInt(out, vector.getRowId());
      serializeLong(out, vector.dim());
//      serializeInt(out, vector.getType().getNumber()); // no need to record type
      serializeLongFloatVector(out, vectors[i]);
    }
  }

  public static LongFloatVector[] deserializeLongFloatVectors(ByteBuf in) {
    int len = in.readInt();
    LongFloatVector[] values = new LongFloatVector[len];
    for (int i = 0; i < len; i++) {
      int rowId = deserializeInt(in);
      long dim = deserializeLong(in);
//      RowType type = RowType.valueOf(deserializeInt(in));
      values[i] = deserializeLongFloatVector(in, dim);
      values[i].setRowId(rowId);
    }
    return values;
  }

  public static int serializedLongFloatVectorsLen(LongFloatVector[] values) {
    int start = 0;
    int end = values.length;
    int len = 0;
    for (int i = start; i < end; i++) {
      len += serializedLongFloatVectorLen(values[i]);
    }
    return len;
  }

  public static int serializedLongIntVectorLen(LongIntVector vector) {
    int len = 0;
    LongIntVectorStorage storage = vector.getStorage();
    if (storage.isSparse()) {
      len += serializedIntLen(SPARSE_STORAGE_TYPE);
      len += serializedIntLen(storage.size());
      len += storage.size() * (INT_LENGTH + FLOAT_LENGTH);
    } else if (storage.isSorted()) {
      len += serializedIntLen(SORTED_STORAGE_TYPE);
      len += serializedLongsLen(vector.getStorage().getIndices());
      len += serializedIntsLen(vector.getStorage().getValues());
    } else {
      throw new UnsupportedOperationException(
          "Unsupport storage type " + vector.getStorage().getClass());
    }

    return len;
  }

  // LongFloatVector
  private static void serializeLongIntVector(ByteBuf out, LongIntVector vector) {
    LongIntVectorStorage storage = vector.getStorage();
    if (storage.isSparse()) {
      serializeInt(out, SPARSE_STORAGE_TYPE);
      serializeInt(out, storage.size());
      ObjectIterator<Long2IntMap.Entry> iter = storage.entryIterator();
      while (iter.hasNext()) {
        Long2IntMap.Entry e = iter.next();
        serializeLong(out, e.getLongKey());
        serializeFloat(out, e.getIntValue());
      }
    } else if (storage.isSorted()) {
      serializeInt(out, SORTED_STORAGE_TYPE);
      long[] indices = vector.getStorage().getIndices();
      int[] values = vector.getStorage().getValues();
      serializeLongs(out, indices);
      serializeInts(out, values);
    } else {
      throw new UnsupportedOperationException(
          "Unsupport storage type " + vector.getStorage().getClass());
    }
  }

  private static LongIntVector deserializeLongIntVector(ByteBuf in, long dim) {
    int storageType = deserializeInt(in);
    switch (storageType) {

      case SPARSE_STORAGE_TYPE:
        int len = deserializeInt(in);
        Long2IntOpenHashMap idToValueMap = new Long2IntOpenHashMap(len);
        for (int i = 0; i < len; i++) {
          idToValueMap.put(deserializeInt(in), deserializeInt(in));
        }
        return new LongIntVector((int) dim,
            new LongIntSparseVectorStorage((int) dim, idToValueMap));

      case SORTED_STORAGE_TYPE:
        return VFactory
            .sortedLongKeyIntVector((int) dim, deserializeLongs(in), deserializeInts(in));

      default:
        throw new UnsupportedOperationException("Unsupport storage type " + storageType);
    }
  }

  // LongFloatVector array
  public static void serializeLongIntVectors(ByteBuf out, LongIntVector[] vectors) {
    int start = 0;
    int end = vectors.length;
    serializeInt(out, end - start);
    for (int i = start; i < end; i++) {
      LongIntVector vector = vectors[i];
      serializeInt(out, vector.getRowId());
      serializeLong(out, vector.dim());
//      serializeInt(out, vector.getType().getNumber()); // no need to record type
      serializeLongIntVector(out, vectors[i]);
    }
  }

  public static LongIntVector[] deserializeLongIntVectors(ByteBuf in) {
    int len = in.readInt();
    LongIntVector[] values = new LongIntVector[len];
    for (int i = 0; i < len; i++) {
      int rowId = deserializeInt(in);
      long dim = deserializeLong(in);
//      RowType type = RowType.valueOf(deserializeInt(in));
      values[i] = deserializeLongIntVector(in, dim);
      values[i].setRowId(rowId);
    }
    return values;
  }

  public static int serializedLongIntVectorsLen(LongIntVector[] values) {
    int start = 0;
    int end = values.length;
    int len = 0;
    for (int i = start; i < end; i++) {
      len += serializedLongIntVectorLen(values[i]);
    }
    return len;
  }


  public static void serializeServerRow(ByteBuf out, ServerRow serverRow) {
    ByteBufSerdeUtils.serializeInt(out, serverRow.getRowType().getNumber());
    serverRow.serialize(out);
  }

  public static ServerRow deserializeServerRow(ByteBuf in) {
    ServerRow rowSplit = ServerRowFactory.createEmptyServerRow(RowType.valueOf(in.readInt()));
    rowSplit.deserialize(in);
    return rowSplit;
  }

  public static int serializedServerRowLen(ServerRow serverRow) {
    return INT_LENGTH + serverRow.bufferLen();
  }

  public static void serializeObject(ByteBuf out, Serialize obj) {
    serializeUTF8(out, obj.getClass().getName());
    obj.serialize(out);
  }

  public static Serialize deserializeObject(ByteBuf in) {
    String className = deserializeUTF8(in);
    try {
      Serialize obj = (Serialize) Class.forName(className).newInstance();
      obj.deserialize(in);
      return obj;
    } catch (Throwable e) {
      throw new RuntimeException("Can not init class " + className, e);
    }
  }

  public static int serializedObjectLen(Serialize obj) {
    return serializedUTF8Len(obj.getClass().getName()) + obj.bufferLen();
  }

  public static void serializeObjects(ByteBuf out, IElement[] objs, int startPos, int endPos) {
    serializeInt(out, endPos - startPos);
    if (endPos - startPos > 0) {
      serializeUTF8(out, objs[startPos].getClass().getName());
      for (int i = startPos; i < endPos; i++) {
        if (objs[i] != null) {
          serializeBoolean(out, true);
          objs[i].serialize(out);
        } else {
          serializeBoolean(out, false);
        }
      }
    }
  }

  public static int serializedObjectsLen(IElement[] objs, int startPos, int endPos) {
    int len = serializedIntLen(endPos - startPos);
    if (endPos - startPos > 0) {
      len += serializedUTF8Len(objs[startPos].getClass().getName());
      for (int i = startPos; i < endPos; i++) {
        len += BOOLEN_LENGTH;
        if (objs[i] != null) {
          len += objs[i].bufferLen();
        }
      }
    }
    return len;
  }

  public static void serializeObjects(ByteBuf out, IElement[] objs) {
    serializeObjects(out, objs, 0, objs.length);
  }

  public static IElement[] deserializeObjects(ByteBuf in) {
    int size = ByteBufSerdeUtils.deserializeInt(in);
    if (size > 0) {
      String className = deserializeUTF8(in);
      try {
        Class objClass = Class.forName(className);
        IElement[] objs = new IElement[size];
        for (int i = 0; i < size; i++) {
          boolean notNull = deserializeBoolean(in);
          if (notNull) {
            objs[i] = (IElement) objClass.newInstance();
            objs[i].deserialize(in);
          }
        }
        return objs;
      } catch (Throwable e) {
        throw new RuntimeException("Can not init class " + className, e);
      }
    } else {
      return new IElement[0];
    }
  }

  public static int serializedObjectsLen(IElement[] objs) {
    return serializedObjectsLen(objs, 0, objs.length);
  }


  //////////////////////////////////////////////////////////////////////////////
  public static void serializeObjects(ByteBuf out, Class<? extends IElement> elemClass,
      IElement[] objs, int startPos, int endPos) {
    serializeInt(out, endPos - startPos);
    if (endPos - startPos > 0) {
      serializeUTF8(out, elemClass.getName());
      for (int i = startPos; i < endPos; i++) {
        if (objs[i] != null) {
          serializeBoolean(out, true);
          objs[i].serialize(out);
        } else {
          serializeBoolean(out, false);
        }
      }
    }
  }

  public static int serializedObjectsLen(Class<? extends IElement> elemClass, IElement[] objs,
      int startPos, int endPos) {
    int len = serializedIntLen(endPos - startPos);
    if (endPos - startPos > 0) {
      len += serializedUTF8Len(elemClass.getName());
      for (int i = startPos; i < endPos; i++) {
        len += BOOLEN_LENGTH;
        if (objs[i] != null) {
          len += objs[i].bufferLen();
        }
      }
    }
    return len;
  }

  public static void serializeObjects(ByteBuf out, Class<? extends IElement> elemClass,
      IElement[] objs) {
    serializeObjects(out, elemClass, objs, 0, objs.length);
  }

  public static int serializedObjectsLen(Class<? extends IElement> elemClass, IElement[] objs) {
    return serializedObjectsLen(elemClass, objs, 0, objs.length);
  }

  public static void serializeKeyPart(ByteBuf out, KeyPart keyPart) {
    serializeInt(out, keyPart.getRouterType().getTypeId());
    serializeInt(out, keyPart.getKeyType().getTypeId());
    keyPart.serialize(out);
  }

  public static KeyPart deserializeKeyPart(ByteBuf in) {
    RouterType routerType = RouterType.valueOf(deserializeInt(in));
    KeyType keyType = KeyType.valueOf(deserializeInt(in));
    KeyPart keyPart = DataPartFactory.createKeyPart(keyType, routerType);
    keyPart.deserialize(in);
    return keyPart;
  }

  public static int serializedKeyPartLen(KeyPart keyPart) {
    return INT_LENGTH * 2 + keyPart.bufferLen();
  }

  public static void serializeKeyValuePart(ByteBuf out, KeyValuePart keyValuePart) {
    serializeBoolean(out, keyValuePart.isComp());
    if (!keyValuePart.isComp()) {
      serializeInt(out, keyValuePart.getRouterType().getTypeId());
      serializeInt(out, keyValuePart.getKeyValueType().getNumber());
    }
    keyValuePart.serialize(out);
  }

  public static KeyValuePart deserializeKeyValuePart(ByteBuf in) {
    boolean isComp = ByteBufSerdeUtils.deserializeBoolean(in);
    KeyValuePart keyValuePart;
    if (isComp) {
      keyValuePart = new CompStreamKeyValuePart();
    } else {
      RouterType routerType = RouterType.valueOf(deserializeInt(in));
      RowType keyValueType = RowType.valueOf(deserializeInt(in));
      keyValuePart = DataPartFactory.createKeyValuePart(keyValueType, routerType);
    }
    keyValuePart.deserialize(in);
    return keyValuePart;
  }

  public static int serializedKeyValuePartLen(KeyValuePart keyValuePart) {
    int len = BOOLEN_LENGTH;
    if (!keyValuePart.isComp()) {
      len += INT_LENGTH * 2;
    }
    len += keyValuePart.bufferLen();
    return len;
  }

  public static void serializeValuePart(ByteBuf out, ValuePart valuePart) {
    serializeInt(out, valuePart.getValueType().getTypeId());
    valuePart.serialize(out);
  }

  public static ValuePart deserializeValuePart(ByteBuf in) {
    int typeCode = deserializeInt(in);
    ValueType valueType = ValueType.valueOf(typeCode);
    ValuePart valuePart = DataPartFactory.createValuePart(valueType);
    valuePart.deserialize(in);
    return valuePart;
  }

  public static int serializedValuePartLen(ValuePart valuePart) {
    return INT_LENGTH + valuePart.bufferLen();
  }

  public static int serializedValueLen(ValueType valueType) {
    switch (valueType) {
      case INT:
        return INT_LENGTH;
      case LONG:
        return LONG_LENGTH;
      case FLOAT:
        return FLOAT_LENGTH;
      case DOUBLE:
        return DOUBLE_LENGTH;

      default:
        throw new UnsupportedOperationException("Only support basic type");
    }
  }

  public static void main(String[] args) {
    boolean b = true;
    ByteBuf buf = ByteBufUtils.newByteBuf(serializedBooleanLen(b), true);
    serializeBoolean(buf, b);
    assert serializedBooleanLen(b) == buf.writerIndex();
    boolean db = deserializeBoolean(buf);
    assert b == db;
    buf.release();

    byte by = 1;
    buf = ByteBufUtils.newByteBuf(serializedByteLen(by), true);
    serializeByte(buf, by);
    assert serializedByteLen(by) == buf.writerIndex();
    byte dby = deserializeByte(buf);
    assert by == dby;
    buf.release();

    char c = 'a';
    buf = ByteBufUtils.newByteBuf(serializedCharLen(c), true);
    serializeChar(buf, c);
    assert serializedCharLen(c) == buf.writerIndex();
    char dc = deserializeChar(buf);
    assert c == dc;
    buf.release();

    int i = 124;
    buf = ByteBufUtils.newByteBuf(serializedIntLen(i), true);
    serializeInt(buf, i);
    assert serializedIntLen(i) == buf.writerIndex();
    int di = deserializeInt(buf);
    assert i == di;
    buf.release();

    long l = 124L;
    buf = ByteBufUtils.newByteBuf(serializedLongLen(l), true);
    serializeLong(buf, l);
    assert serializedLongLen(l) == buf.writerIndex();
    long dl = deserializeLong(buf);
    assert l == dl;
    buf.release();

    float f = 12.34f;
    buf = ByteBufUtils.newByteBuf(serializedFloatLen(f), true);
    serializeFloat(buf, f);
    assert serializedFloatLen(f) == buf.writerIndex();
    float df = deserializeFloat(buf);
    assert f == df;

    double d = 12.34;
    buf = ByteBufUtils.newByteBuf(serializedDoubleLen(d), true);
    serializeDouble(buf, d);
    assert serializedDoubleLen(d) == buf.writerIndex();
    double dd = deserializeDouble(buf);
    assert d == dd;
    buf.release();

    String s = "123";
    buf = ByteBufUtils.newByteBuf(serializedUTF8Len(s), true);
    serializeUTF8(buf, s);
    assert serializedUTF8Len(s) == buf.writerIndex();
    String ds = deserializeUTF8(buf);
    assert s.equalsIgnoreCase(ds);
    buf.release();

    int[] ia = new int[3];
    for (i = 0; i < ia.length; i++) {
      ia[i] = i;
    }
    buf = ByteBufUtils.newByteBuf(serializedIntsLen(ia, 0, ia.length), true);
    serializeInts(buf, ia);
    assert serializedIntsLen(ia, 0, ia.length) == buf.writerIndex();
    int[] dia = deserializeInts(buf);
    assert ia.length == dia.length;
    for (i = 0; i < ia.length; i++) {
      assert ia[i] == dia[i];
    }
    buf.release();

    float[] fa = new float[3];
    for (i = 0; i < ia.length; i++) {
      fa[i] = i;
    }
    buf = ByteBufUtils.newByteBuf(serializedFloatsLen(fa, 0, fa.length), true);
    serializeFloats(buf, fa);
    assert serializedFloatsLen(fa, 0, fa.length) == buf.writerIndex();
    float[] dfa = deserializeFloats(buf);
    assert fa.length == dfa.length;
    for (i = 0; i < fa.length; i++) {
      assert fa[i] == dfa[i];
    }
    buf.release();

    double[] da = new double[3];
    for (i = 0; i < ia.length; i++) {
      da[i] = i;
    }
    buf = ByteBufUtils.newByteBuf(serializedDoublesLen(da, 0, da.length), true);
    serializeDoubles(buf, da, 0, da.length);
    assert serializedDoublesLen(da, 0, fa.length) == buf.writerIndex();
    double[] dda = deserializeDoubles(buf);
    assert da.length == dda.length;
    for (i = 0; i < da.length; i++) {
      assert da[i] == dda[i];
    }
    buf.release();

    long[] la = new long[3];
    for (i = 0; i < la.length; i++) {
      la[i] = i;
    }
    buf = ByteBufUtils.newByteBuf(serializedLongsLen(la, 0, la.length), true);
    serializeLongs(buf, la, 0, da.length);
    assert serializedLongsLen(la, 0, la.length) == buf.writerIndex();
    long[] dla = deserializeLongs(buf);
    assert la.length == dla.length;
    for (i = 0; i < la.length; i++) {
      assert la[i] == dla[i];
    }
    buf.release();

    IntDoubleVector didVector = VFactory.denseDoubleVector(da);
    buf = ByteBufUtils.newByteBuf(serializedVectorLen(didVector));
    serializeVector(buf, didVector);
    assert serializedVectorLen(didVector) == buf.writerIndex();
    IntDoubleVector ddidVector = (IntDoubleVector) deserializeVector(buf);
    for (i = 0; i < da.length; i++) {
      assert ddidVector.get(i) == da[i];
    }
  }


  public static void serializePart(ByteBuf buf, PartitionKey partKey) {
    partKey.serialize(buf);
  }

  public static PartitionKey deserializePart(ByteBuf in) {
    PartitionKey partKey = new PartitionKey();
    partKey.deserialize(in);
    return partKey;
  }

  public static int serializedPartLen(PartitionKey partKey) {
    return partKey.bufferLen();
  }

  public static void serializeEmptyFloats(ByteBuf output) {
    serializeFloats(output, emptyFloats);
  }

  public static int serializedEmptyFloatsLen() {
    return serializedFloatsLen(emptyFloats);
  }
}

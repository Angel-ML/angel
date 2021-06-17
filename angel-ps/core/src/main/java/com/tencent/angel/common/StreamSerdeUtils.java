package com.tencent.angel.common;

import static com.tencent.angel.common.ByteBufSerdeUtils.emptyFloats;

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
import com.tencent.angel.ps.server.data.request.ValueType;
import com.tencent.angel.ps.storage.vector.ServerRow;
import com.tencent.angel.ps.storage.vector.ServerRowFactory;
import com.tencent.angel.ps.storage.vector.element.IElement;
import com.tencent.angel.psagent.matrix.transport.router.ValuePart;
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
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class StreamSerdeUtils {

  public static final int BOOLEN_LENGTH = 1;
  public static final int BYTE_LENGTH = 1;
  public static final int CHAR_LENGTH = 2;
  public static final int INT_LENGTH = 4;
  public static final int LONG_LENGTH = 8;
  public static final int FLOAT_LENGTH = 4;
  public static final int DOUBLE_LENGTH = 8;

  // =======================================================
  // Boolean
  public static void serializeBoolean(DataOutputStream out, boolean value) throws IOException {
    out.writeBoolean(value);
  }

  public static boolean deserializeBoolean(DataInputStream in) throws IOException {
    return in.readBoolean();
  }

  public static int serializedBooleanLen(boolean value) {
    return BOOLEN_LENGTH;
  }

  // =======================================================
  // Byte
  public static void serializeByte(DataOutputStream out, byte value) throws IOException {
    out.writeByte(value);
  }

  public static byte deserializeByte(DataInputStream in) throws IOException {
    return in.readByte();
  }

  public static int serializedByteLen(byte value) {
    return BYTE_LENGTH;
  }

  // =======================================================
  // Int
  public static void serializeChar(DataOutputStream out, int value) throws IOException {
    out.writeChar(value);
  }

  public static char deserializeChar(DataInputStream in) throws IOException {
    return in.readChar();
  }

  public static int serializedCharLen(char value) {
    return CHAR_LENGTH;
  }

  // =======================================================
  // Int
  public static void serializeInt(DataOutputStream out, int value) throws IOException {
    out.writeInt(value);
  }

  public static int deserializeInt(DataInputStream in) throws IOException {
    return in.readInt();
  }

  public static int serializedIntLen(int value) {
    return INT_LENGTH;
  }

  // =======================================================
  // Long
  public static void serializeLong(DataOutputStream out, long value) throws IOException {
    out.writeLong(value);
  }

  public static long deserializeLong(DataInputStream in) throws IOException {
    return in.readLong();
  }

  public static int serializedLongLen(long value) {
    return LONG_LENGTH;
  }

  // =======================================================
  // Float
  public static void serializeFloat(DataOutputStream out, float value) throws IOException {
    out.writeFloat(value);
  }

  public static float deserializeFloat(DataInputStream in) throws IOException {
    return in.readFloat();
  }

  public static int serializedFloatLen(float value) {
    return FLOAT_LENGTH;
  }

  // =======================================================
  // String
  public static void serializeUTF8(DataOutputStream out, String value) throws IOException {
    try {
      byte[] strBytes = value.getBytes("UTF-8");
      serializeBytes(out, strBytes);
    } catch (UnsupportedEncodingException e) {
      throw new IOException (
          "Serialize " + value + " failed, details = " + StringUtils.stringifyException(e));
    }
  }

  public static String deserializeUTF8(DataInputStream in) throws IOException {
    try {
      return new String(deserializeBytes(in), "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new IOException(
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
  public static void serializeDouble(DataOutputStream out, double value) throws IOException {
    out.writeDouble(value);
  }

  public static double deserializeDouble(DataInputStream in) throws IOException {
    return in.readDouble();
  }

  public static int serializedDoubleLen(double value) {
    return DOUBLE_LENGTH;
  }

  // =======================================================
  // Byte array
  public static void serializeBytes(DataOutputStream out, byte[] values, int start, int end) throws IOException {
    serializeInt(out, end - start);
    out.write(values, start, end - start);
  }

  public static void serializeBytes(DataOutputStream out, byte[] values) throws IOException {
    serializeBytes(out, values, 0, values.length);
  }

  public static byte[] deserializeBytes(DataInputStream in) throws IOException {
    int len = in.readInt();
    byte[] values = new byte[len];
    in.readFully(values);
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
  public static void serializeInts(DataOutputStream out, int[] values, int start, int end) throws IOException {
    serializeInt(out, end - start);
    for (int i = start; i < end; i++) {
      serializeInt(out, values[i]);
    }
  }

  public static void serializeInts(DataOutputStream out, int[] values) throws IOException {
    serializeInts(out, values, 0, values.length);
  }

  public static int[] deserializeInts(DataInputStream in) throws IOException {
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
  public static void serializeLongs(DataOutputStream out, long[] values, int start, int end) throws IOException {
    serializeInt(out, end - start);
    for (int i = start; i < end; i++) {
      serializeLong(out, values[i]);
    }
  }

  public static void serializeLongs(DataOutputStream out, long[] values) throws IOException {
    serializeLongs(out, values, 0, values.length);
  }

  public static long[] deserializeLongs(DataInputStream in) throws IOException {
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
  public static void serializeFloats(DataOutputStream out, float[] values, int start, int end) throws IOException {
    serializeInt(out, end - start);
    for (int i = start; i < end; i++) {
      serializeFloat(out, values[i]);
    }
  }

  public static void serializeFloats(DataOutputStream out, float[] values) throws IOException {
    serializeFloats(out, values, 0, values.length);
  }

  public static float[] deserializeFloats(DataInputStream in) throws IOException {
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
  public static void serializeDoubles(DataOutputStream out, double[] values, int start, int end) throws IOException {
    serializeInt(out, end - start);
    for (int i = start; i < end; i++) {
      serializeDouble(out, values[i]);
    }
  }

  public static void serializeDoubles(DataOutputStream out, double[] values) throws IOException {
    serializeDoubles(out, values, 0, values.length);
  }

  public static double[] deserializeDoubles(DataInputStream in) throws IOException {
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
  public static void serializeUTF8s(DataOutputStream out, String[] values) throws IOException {
    serializeInt(out, values.length);
    for (int i = 0; i < values.length; i++) {
      serializeUTF8(out, values[i]);
    }
  }

  public static String[] deserializeUTF8s(DataInputStream in) throws IOException {
    int len = deserializeInt(in);
    String[] values = new String[len];
    for (int i = 0; i < len; i++) {
      values[i] = deserializeUTF8(in);
    }
    return values;
  }

  public static int serializedUTF8sLen(String[] values) {
    int len = serializedIntLen(values.length);
    for (int i = 0; i < len; i++) {
      len += serializedUTF8Len(values[i]);
    }
    return len;
  }

  // =======================================================
  // 2-D Int array
  public static void serialize2DInts(DataOutputStream out, int[][] values, int start, int end) throws IOException {
    serializeInt(out, end - start);
    for (int i = start; i < end; i++) {
      serializeInts(out, values[i]);
    }
  }

  public static void serialize2DInts(DataOutputStream out, int[][] values) throws IOException {
    serialize2DInts(out, values, 0, values.length);
  }

  public static int[][] deserialize2DInts(DataInputStream in) throws IOException {
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
  public static void serialize2DLongs(DataOutputStream out, long[][] values, int start, int end) throws IOException {
    serializeInt(out, end - start);
    for (int i = start; i < end; i++) {
      serializeLongs(out, values[i]);
    }
  }

  public static void serialize2DLongs(DataOutputStream out, long[][] values) throws IOException {
    serialize2DLongs(out, values, 0, values.length);
  }

  public static long[][] deserialize2DLongs(DataInputStream in) throws IOException {
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
  public static void serialize2DFloats(DataOutputStream out, float[][] values, int start, int end) throws IOException {
    serializeInt(out, end - start);
    for (int i = start; i < end; i++) {
      serializeFloats(out, values[i]);
    }
  }

  public static void serialize2DFloats(DataOutputStream out, float[][] values) throws IOException {
    serialize2DFloats(out, values, 0, values.length);
  }

  public static float[][] deserialize2DFloats(DataInputStream in) throws IOException {
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
  public static void serialize2DDoubles(DataOutputStream out, double[][] values, int start, int end) throws IOException {
    serializeInt(out, end - start);
    for (int i = start; i < end; i++) {
      serializeDoubles(out, values[i]);
    }
  }

  public static void serialize2DDoubles(DataOutputStream out, double[][] values) throws IOException {
    serialize2DDoubles(out, values, 0, values.length);
  }

  public static double[][] deserialize2DDoubles(DataInputStream in) throws IOException {
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

  public static void serializeVector(DataOutputStream out, Vector vector) throws IOException {
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

  public static Vector deserializeVector(DataInputStream in) throws IOException {
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
  private static void serializeIntDoubleVector(DataOutputStream out, IntDoubleVector vector) throws IOException {
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

  private static IntDoubleVector deserializeIntDoubleVector(DataInputStream in, long dim)
      throws IOException {
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
  private static void serializeIntFloatVector(DataOutputStream out, IntFloatVector vector) throws IOException {
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

  private static IntFloatVector deserializeIntFloatVector(DataInputStream in, long dim)
      throws IOException {
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
  public static void serializeIntFloatVectors(DataOutputStream out, IntFloatVector[] vectors) throws IOException {
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

  public static IntFloatVector[] deserializeIntFloatVectors(DataInputStream in) throws IOException {
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

  public static int serializedIntFloatVectorsLen(IntFloatVector[] values)  {
    int start = 0;
    int end = values.length;
    int len = 0;
    for (int i = start; i < end; i++) {
      len += serializedIntFloatVectorLen(values[i]);
    }
    return len;
  }

  public static int serializedLongFloatVectorLen(LongFloatVector vector)  {
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
  private static void serializeLongFloatVector(DataOutputStream out, LongFloatVector vector) throws IOException {
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

  private static LongFloatVector deserializeLongFloatVector(DataInputStream in, long dim) throws IOException {
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
        return VFactory.sortedLongKeyFloatVector((int) dim, deserializeLongs(in), deserializeFloats(in));

      default:
        throw new UnsupportedOperationException("Unsupport storage type " + storageType);
    }
  }

  // LongFloatVector array
  public static void serializeLongFloatVectors(DataOutputStream out, LongFloatVector[] vectors) throws IOException {
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

  public static LongFloatVector[] deserializeLongFloatVectors(DataInputStream in) throws IOException {
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
  private static void serializeLongIntVector(DataOutputStream out, LongIntVector vector) throws IOException {
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

  private static LongIntVector deserializeLongIntVector(DataInputStream in, long dim)
      throws IOException {
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
        return VFactory.sortedLongKeyIntVector((int) dim, deserializeLongs(in), deserializeInts(in));

      default:
        throw new UnsupportedOperationException("Unsupport storage type " + storageType);
    }
  }

  // LongFloatVector array
  public static void serializeLongIntVectors(DataOutputStream out, LongIntVector[] vectors) throws IOException {
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

  public static LongIntVector[] deserializeLongIntVectors(DataInputStream in) throws IOException {
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


  public static void serializeServerRow(DataOutputStream out, ServerRow serverRow)
      throws IOException {
    serializeInt(out, serverRow.getRowType().getNumber());
    serverRow.serialize(out);
  }

  public static ServerRow deserializeServerRow(DataInputStream in) throws IOException {
    ServerRow rowSplit = ServerRowFactory.createEmptyServerRow(RowType.valueOf(in.readInt()));
    rowSplit.deserialize(in);
    return rowSplit;
  }

  public static int serializedServerRowLen(ServerRow serverRow) {
    return INT_LENGTH + serverRow.bufferLen();
  }

  public static void serializeObject(DataOutputStream out, StreamSerialize obj) throws IOException {
    serializeUTF8(out, obj.getClass().getName());
    obj.serialize(out);
  }

  public static StreamSerialize deserializeObject(DataInputStream in) throws IOException {
    String className = deserializeUTF8(in);
    try {
      StreamSerialize obj = (StreamSerialize) Class.forName(className).newInstance();
      obj.deserialize(in);
      return obj;
    } catch (Throwable e) {
      throw new IOException("Can not init class " + className);
    }
  }

  public static int serializedObjectLen(Serialize obj) {
    return serializedUTF8Len(obj.getClass().getName()) + obj.bufferLen();
  }

  public static void serializeObjects(DataOutputStream out, IElement[] objs, int startPos, int endPos)
      throws IOException {
    serializeInt(out, endPos - startPos);
    if (endPos - startPos > 0) {
      serializeUTF8(out, objs[startPos].getClass().getName());
      for (int i = startPos; i < startPos; i++) {
        objs[i].serialize(out);
      }
    }
  }

  public static int serializedObjectsLen(IElement[] objs, int startPos, int endPos) {
    int len = serializedIntLen(endPos - startPos);
    if (endPos - startPos > 0) {
      len += serializedUTF8Len(objs[startPos].getClass().getName());
      for (int i = startPos; i < endPos; i++) {
        len += objs[i].bufferLen();
      }
    }
    return len;
  }

  public static void serializeObjects(DataOutputStream out, IElement[] objs) throws IOException {
    serializeObjects(out, objs, 0, objs.length);
  }

  public static IElement[] deserializeObjects(DataInputStream in) throws IOException {
    int size = deserializeInt(in);
    if (size > 0) {
      String className = deserializeUTF8(in);
      try {
        Class objClass = Class.forName(className);
        int len = deserializeInt(in);
        IElement[] objs = new IElement[len];
        for (int i = 0; i < len; i++) {
          objs[i] = (IElement) objClass.newInstance();
          objs[i].deserialize(in);
        }
        return objs;
      } catch (Throwable e) {
        throw new RuntimeException("Can not init class " + className);
      }
    } else {
      return new IElement[0];
    }
  }

  public static int serializedObjectsLen(IElement[] objs) {
    return serializedObjectsLen(objs, 0, objs.length);
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
  }

  public static void serializeEmptyFloats(DataOutputStream output) throws IOException {
    serializeFloats(output, emptyFloats);
  }

  public static int serializedEmptyFloatsLen() {
    return serializedFloatsLen(emptyFloats);
  }
}

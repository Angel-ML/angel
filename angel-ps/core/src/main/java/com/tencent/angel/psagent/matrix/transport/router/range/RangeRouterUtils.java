package com.tencent.angel.psagent.matrix.transport.router.range;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.math2.storage.IntDoubleDenseVectorStorage;
import com.tencent.angel.ml.math2.storage.IntDoubleSortedVectorStorage;
import com.tencent.angel.ml.math2.storage.IntDoubleSparseVectorStorage;
import com.tencent.angel.ml.math2.storage.IntDoubleVectorStorage;
import com.tencent.angel.ml.math2.storage.IntFloatDenseVectorStorage;
import com.tencent.angel.ml.math2.storage.IntFloatSortedVectorStorage;
import com.tencent.angel.ml.math2.storage.IntFloatSparseVectorStorage;
import com.tencent.angel.ml.math2.storage.IntFloatVectorStorage;
import com.tencent.angel.ml.math2.storage.IntIntDenseVectorStorage;
import com.tencent.angel.ml.math2.storage.IntIntSortedVectorStorage;
import com.tencent.angel.ml.math2.storage.IntIntSparseVectorStorage;
import com.tencent.angel.ml.math2.storage.IntIntVectorStorage;
import com.tencent.angel.ml.math2.storage.IntLongDenseVectorStorage;
import com.tencent.angel.ml.math2.storage.IntLongSortedVectorStorage;
import com.tencent.angel.ml.math2.storage.IntLongSparseVectorStorage;
import com.tencent.angel.ml.math2.storage.IntLongVectorStorage;
import com.tencent.angel.ml.math2.storage.LongDoubleSortedVectorStorage;
import com.tencent.angel.ml.math2.storage.LongDoubleSparseVectorStorage;
import com.tencent.angel.ml.math2.storage.LongDoubleVectorStorage;
import com.tencent.angel.ml.math2.storage.LongFloatSortedVectorStorage;
import com.tencent.angel.ml.math2.storage.LongFloatSparseVectorStorage;
import com.tencent.angel.ml.math2.storage.LongFloatVectorStorage;
import com.tencent.angel.ml.math2.storage.LongIntSortedVectorStorage;
import com.tencent.angel.ml.math2.storage.LongIntSparseVectorStorage;
import com.tencent.angel.ml.math2.storage.LongIntVectorStorage;
import com.tencent.angel.ml.math2.storage.LongLongSortedVectorStorage;
import com.tencent.angel.ml.math2.storage.LongLongSparseVectorStorage;
import com.tencent.angel.ml.math2.storage.LongLongVectorStorage;
import com.tencent.angel.ml.math2.vector.IntDoubleVector;
import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ml.math2.vector.IntIntVector;
import com.tencent.angel.ml.math2.vector.IntLongVector;
import com.tencent.angel.ml.math2.vector.LongDoubleVector;
import com.tencent.angel.ml.math2.vector.LongFloatVector;
import com.tencent.angel.ml.math2.vector.LongIntVector;
import com.tencent.angel.ml.math2.vector.LongLongVector;
import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ps.storage.vector.element.IElement;
import com.tencent.angel.psagent.matrix.transport.router.CompStreamKeyValuePart;
import com.tencent.angel.psagent.matrix.transport.router.KeyPart;
import com.tencent.angel.psagent.matrix.transport.router.KeyValuePart;
import com.tencent.angel.utils.Sort;
import java.util.Arrays;

public class RangeRouterUtils {

  /////////////////////////////////////////////////////////////////////////////////////////
  // keys split: range split only support int/long key
  /////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Split keys by matrix partition
   *
   * @param matrixMeta matrix meta data
   * @param keys the keys need partitioned
   * @return partition key to key partition map
   */
  public static KeyPart[] split(MatrixMeta matrixMeta, int rowId, int[] keys, boolean isSorted) {
    if (!isSorted) {
      Arrays.sort(keys);
    }

    if(keys[keys.length - 1] >= matrixMeta.getColNum()) {
      System.out.println("Error happened!!!");
    }

    PartitionKey[] matrixParts = matrixMeta.getPartitionKeys();
    KeyPart[] dataParts = new KeyPart[matrixParts.length];

    int keyIndex = 0;
    int partIndex = 0;
    while (keyIndex < keys.length || partIndex < matrixParts.length) {
      int length = 0;
      long endOffset = matrixParts[partIndex].getEndCol();
      while (keyIndex < keys.length && keys[keyIndex] < endOffset) {
        keyIndex++;
        length++;
      }

      if (length != 0) {
        dataParts[partIndex] = new RangeViewIntKeysPart(rowId, keys, keyIndex - length, keyIndex);
      }
      partIndex++;
    }
    return dataParts;
  }

  /**
   * Split keys by matrix partition
   *
   * @param matrixMeta matrix meta data
   * @param keys the keys need partitioned
   * @return partition key to key partition map
   */
  public static KeyPart[] split(MatrixMeta matrixMeta, int rowId, long[] keys, boolean isSorted) {
    if (!isSorted) {
      Arrays.sort(keys);
    }

    PartitionKey[] matrixParts = matrixMeta.getPartitionKeys();
    KeyPart[] dataParts = new KeyPart[matrixParts.length];

    int keyIndex = 0;
    int partIndex = 0;
    while (keyIndex < keys.length || partIndex < matrixParts.length) {
      int length = 0;
      long endOffset = matrixParts[partIndex].getEndCol();
      while (keyIndex < keys.length && keys[keyIndex] < endOffset) {
        keyIndex++;
        length++;
      }

      if (length != 0) {
        dataParts[partIndex] = new RangeViewLongKeysPart(rowId, keys, keyIndex - length, keyIndex);
      }
      partIndex++;
    }
    return dataParts;
  }

  /**
   * Split keys by matrix partition
   *
   * @param matrixMeta matrix meta data
   * @param keys the keys need partitioned
   * @return partition key to key partition map
   */
  public static KeyPart[] split(MatrixMeta matrixMeta, int rowId, String[] keys, boolean isSorted) {
    throw new UnsupportedOperationException("Unsupport String key in range partition");
  }

  /**
   * Split keys by matrix partition
   *
   * @param matrixMeta matrix meta data
   * @param keys the keys need partitioned
   * @return partition key to key partition map
   */
  public static KeyPart[] split(MatrixMeta matrixMeta, int rowId, IElement[] keys,
      boolean isSorted) {
    throw new UnsupportedOperationException("Unsupport IElement key in range partition");
  }

  /////////////////////////////////////////////////////////////////////////////////////////
  // keys/values pair split: int key, float/double/int/long/any values
  /////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Split keys by matrix partition
   *
   * @param matrixMeta matrix meta data
   * @param keys the keys need partitioned
   * @return partition key to key partition map
   */
  public static KeyValuePart[] split(MatrixMeta matrixMeta, int rowId, int[] keys,
      float[] values, boolean isSorted) {
    if (!isSorted) {
      Sort.quickSort(keys, values, 0, keys.length - 1);
    }

    PartitionKey[] matrixParts = matrixMeta.getPartitionKeys();
    KeyValuePart[] dataParts = new KeyValuePart[matrixParts.length];

    int keyIndex = 0;
    int partIndex = 0;
    while (keyIndex < keys.length || partIndex < matrixParts.length) {
      int length = 0;
      long endOffset = matrixParts[partIndex].getEndCol();
      while (keyIndex < keys.length && keys[keyIndex] < endOffset) {
        keyIndex++;
        length++;
      }

      if (length != 0) {
        dataParts[partIndex] = new RangeViewIntKeysFloatValuesPart(rowId, keys, values,
            keyIndex - length, keyIndex);
      }
      partIndex++;
    }
    return dataParts;
  }

  /**
   * Split keys by matrix partition
   *
   * @param matrixMeta matrix meta data
   * @param keys the keys need partitioned
   * @return partition key to key partition map
   */
  public static KeyValuePart[] split(MatrixMeta matrixMeta, int rowId, int[] keys,
      double[] values, boolean isSorted) {
    if (!isSorted) {
      Sort.quickSort(keys, values, 0, keys.length - 1);
    }

    PartitionKey[] matrixParts = matrixMeta.getPartitionKeys();
    KeyValuePart[] dataParts = new KeyValuePart[matrixParts.length];

    int keyIndex = 0;
    int partIndex = 0;
    while (keyIndex < keys.length || partIndex < matrixParts.length) {
      int length = 0;
      long endOffset = matrixParts[partIndex].getEndCol();
      while (keyIndex < keys.length && keys[keyIndex] < endOffset) {
        keyIndex++;
        length++;
      }

      dataParts[partIndex] = new RangeViewIntKeysDoubleValuesPart(rowId, keys, values,
          keyIndex - length, keyIndex);
      partIndex++;
    }
    return dataParts;
  }

  /**
   * Split keys/values by matrix partition
   *
   * @param matrixMeta matrix meta data
   * @param keys the keys need partitioned
   * @return partition key to key partition map
   */
  public static KeyValuePart[] split(MatrixMeta matrixMeta, int rowId, int[] keys,
      int[] values, boolean isSorted) {
    if (!isSorted) {
      Sort.quickSort(keys, values, 0, keys.length - 1);
    }

    PartitionKey[] matrixParts = matrixMeta.getPartitionKeys();
    KeyValuePart[] dataParts = new KeyValuePart[matrixParts.length];

    int keyIndex = 0;
    int partIndex = 0;
    while (keyIndex < keys.length || partIndex < matrixParts.length) {
      int length = 0;
      long endOffset = matrixParts[partIndex].getEndCol();
      while (keyIndex < keys.length && keys[keyIndex] < endOffset) {
        keyIndex++;
        length++;
      }

      dataParts[partIndex] = new RangeViewIntKeysIntValuesPart(rowId, keys, values,
          keyIndex - length, keyIndex);
      partIndex++;
    }
    return dataParts;
  }

  /**
   * Split keys/values by matrix partition
   *
   * @param matrixMeta matrix meta data
   * @param keys the keys need partitioned
   * @return partition key to key partition map
   */
  public static KeyValuePart[] split(MatrixMeta matrixMeta, int rowId, int[] keys,
      long[] values, boolean isSorted) {
    if (!isSorted) {
      Sort.quickSort(keys, values, 0, keys.length - 1);
    }

    PartitionKey[] matrixParts = matrixMeta.getPartitionKeys();
    KeyValuePart[] dataParts = new KeyValuePart[matrixParts.length];

    int keyIndex = 0;
    int partIndex = 0;
    while (keyIndex < keys.length || partIndex < matrixParts.length) {
      int length = 0;
      long endOffset = matrixParts[partIndex].getEndCol();
      while (keyIndex < keys.length && keys[keyIndex] < endOffset) {
        keyIndex++;
        length++;
      }

      dataParts[partIndex] = new RangeViewIntKeysLongValuesPart(rowId, keys, values,
          keyIndex - length, keyIndex);
      partIndex++;
    }
    return dataParts;
  }

  /**
   * Split keys/values by matrix partition
   *
   * @param matrixMeta matrix meta data
   * @param keys the keys need partitioned
   * @return partition key to key partition map
   */
  public static KeyValuePart[] split(MatrixMeta matrixMeta, int rowId, int[] keys,
      IElement[] values, boolean isSorted) {
    if (!isSorted) {
      Sort.quickSort(keys, values, 0, keys.length - 1);
    }

    PartitionKey[] matrixParts = matrixMeta.getPartitionKeys();
    KeyValuePart[] dataParts = new KeyValuePart[matrixParts.length];

    int keyIndex = 0;
    int partIndex = 0;
    while (keyIndex < keys.length || partIndex < matrixParts.length) {
      int length = 0;
      long endOffset = matrixParts[partIndex].getEndCol();
      while (keyIndex < keys.length && keys[keyIndex] < endOffset) {
        keyIndex++;
        length++;
      }

      dataParts[partIndex] = new RangeViewIntKeysAnyValuesPart(rowId, keys, values,
          keyIndex - length, keyIndex);
      partIndex++;
    }
    return dataParts;
  }

  /////////////////////////////////////////////////////////////////////////////////////////
  // keys/values pair split: long key, float/double/int/long/any values
  /////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Split keys by matrix partition
   *
   * @param matrixMeta matrix meta data
   * @param keys the keys need partitioned
   * @param values the values of the keys
   * @return partition key to key partition map
   */
  public static KeyValuePart[] split(MatrixMeta matrixMeta, int rowId, long[] keys,
      float[] values, boolean isSorted) {
    if (!isSorted) {
      Sort.quickSort(keys, values, 0, keys.length - 1);
    }

    PartitionKey[] matrixParts = matrixMeta.getPartitionKeys();
    KeyValuePart[] dataParts = new KeyValuePart[matrixParts.length];

    int keyIndex = 0;
    int partIndex = 0;
    while (keyIndex < keys.length || partIndex < matrixParts.length) {
      int length = 0;
      long endOffset = matrixParts[partIndex].getEndCol();
      while (keyIndex < keys.length && keys[keyIndex] < endOffset) {
        keyIndex++;
        length++;
      }

      if (length != 0) {
        dataParts[partIndex] = new RangeViewLongKeysFloatValuesPart(rowId, keys, values,
            keyIndex - length, keyIndex);
      }
      partIndex++;
    }
    return dataParts;
  }

  /**
   * Split keys by matrix partition
   *
   * @param matrixMeta matrix meta data
   * @param keys the keys need partitioned
   * @param values the values of the keys
   * @return partition key to key partition map
   */
  public static KeyValuePart[] split(MatrixMeta matrixMeta, int rowId, long[] keys,
      double[] values, boolean isSorted) {
    if (!isSorted) {
      Sort.quickSort(keys, values, 0, keys.length - 1);
    }

    PartitionKey[] matrixParts = matrixMeta.getPartitionKeys();
    KeyValuePart[] dataParts = new KeyValuePart[matrixParts.length];

    int keyIndex = 0;
    int partIndex = 0;
    while (keyIndex < keys.length || partIndex < matrixParts.length) {
      int length = 0;
      long endOffset = matrixParts[partIndex].getEndCol();
      while (keyIndex < keys.length && keys[keyIndex] < endOffset) {
        keyIndex++;
        length++;
      }

      dataParts[partIndex] = new RangeViewLongKeysDoubleValuesPart(rowId, keys, values,
          keyIndex - length, keyIndex);
      partIndex++;
    }
    return dataParts;
  }


  /**
   * Split keys by matrix partition
   *
   * @param matrixMeta matrix meta data
   * @param keys the keys need partitioned
   * @param values the values of the keys
   * @return partition key to key partition map
   */
  public static KeyValuePart[] split(MatrixMeta matrixMeta, int rowId, long[] keys,
      int[] values, boolean isSorted) {
    if (!isSorted) {
      Sort.quickSort(keys, values, 0, keys.length - 1);
    }

    PartitionKey[] matrixParts = matrixMeta.getPartitionKeys();
    KeyValuePart[] dataParts = new KeyValuePart[matrixParts.length];

    int keyIndex = 0;
    int partIndex = 0;
    while (keyIndex < keys.length || partIndex < matrixParts.length) {
      int length = 0;
      long endOffset = matrixParts[partIndex].getEndCol();
      while (keyIndex < keys.length && keys[keyIndex] < endOffset) {
        keyIndex++;
        length++;
      }

      dataParts[partIndex] = new RangeViewLongKeysIntValuesPart(rowId, keys, values,
          keyIndex - length, keyIndex);
      partIndex++;
    }
    return dataParts;
  }

  /**
   * Split keys by matrix partition
   *
   * @param matrixMeta matrix meta data
   * @param keys the keys need partitioned
   * @param values the values of the keys
   * @return partition key to key partition map
   */
  public static KeyValuePart[] split(MatrixMeta matrixMeta, int rowId, long[] keys,
      long[] values, boolean isSorted) {
    if (!isSorted) {
      Sort.quickSort(keys, values, 0, keys.length - 1);
    }

    PartitionKey[] matrixParts = matrixMeta.getPartitionKeys();
    KeyValuePart[] dataParts = new KeyValuePart[matrixParts.length];

    int keyIndex = 0;
    int partIndex = 0;
    while (keyIndex < keys.length || partIndex < matrixParts.length) {
      int length = 0;
      long endOffset = matrixParts[partIndex].getEndCol();
      while (keyIndex < keys.length && keys[keyIndex] < endOffset) {
        keyIndex++;
        length++;
      }

      dataParts[partIndex] = new RangeViewLongKeysLongValuesPart(rowId, keys, values,
          keyIndex - length, keyIndex);
      partIndex++;
    }
    return dataParts;
  }

  /**
   * Split keys by matrix partition
   *
   * @param matrixMeta matrix meta data
   * @param keys the keys need partitioned
   * @param values the values of the keys
   * @return partition key to key partition map
   */
  public static KeyValuePart[] split(MatrixMeta matrixMeta, int rowId, long[] keys,
      IElement[] values, boolean isSorted) {
    if (!isSorted) {
      Sort.quickSort(keys, values, 0, keys.length - 1);
    }

    PartitionKey[] matrixParts = matrixMeta.getPartitionKeys();
    KeyValuePart[] dataParts = new KeyValuePart[matrixParts.length];

    int keyIndex = 0;
    int partIndex = 0;
    while (keyIndex < keys.length || partIndex < matrixParts.length) {
      int length = 0;
      long endOffset = matrixParts[partIndex].getEndCol();
      while (keyIndex < keys.length && keys[keyIndex] < endOffset) {
        keyIndex++;
        length++;
      }

      dataParts[partIndex] = new RangeViewLongKeysAnyValuesPart(rowId, keys, values,
          keyIndex - length, keyIndex);
      partIndex++;
    }
    return dataParts;
  }

  public static KeyValuePart[] split(MatrixMeta matrixMeta, int rowId, String[] keys,
      float[] values, boolean isSorted) {
    throw new UnsupportedOperationException("Unsupport String key in range partition");
  }

  public static KeyValuePart[] split(MatrixMeta matrixMeta, int rowId, IElement[] keys,
      float[] values, boolean isSorted) {
    throw new UnsupportedOperationException("Unsupport IElement key in range partition");
  }

  public static KeyValuePart[] split(MatrixMeta matrixMeta, int rowId, String[] keys,
      double[] values, boolean isSorted) {
    throw new UnsupportedOperationException("Unsupport String key in range partition");
  }

  public static KeyValuePart[] split(MatrixMeta matrixMeta, int rowId, IElement[] keys,
      double[] values, boolean isSorted) {
    throw new UnsupportedOperationException("Unsupport IElement key in range partition");
  }

  public static KeyValuePart[] split(MatrixMeta matrixMeta, int rowId, String[] keys,
      IElement[] values, boolean isSorted) {
    throw new UnsupportedOperationException("Unsupport String key in range partition");
  }

  public static KeyValuePart[] split(MatrixMeta matrixMeta, int rowId, IElement[] keys,
      IElement[] values, boolean isSorted) {
    throw new UnsupportedOperationException("Unsupport IElement key in range partition");
  }

  /////////////////////////////////////////////////////////////////////////////////////////
  // values pair split: float/double/int/long/any values
  /////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Split values
   *
   * @param matrixMeta matrix meta
   * @param rowId row id
   * @param values values
   * @return value splits
   */
  public static KeyValuePart[] split(MatrixMeta matrixMeta, int rowId, double[] values) {
    PartitionKey[] matrixParts = matrixMeta.getPartitionKeys();
    KeyValuePart[] dataParts = new KeyValuePart[matrixParts.length];

    for (int i = 0; i < matrixParts.length; i++) {
      dataParts[i] = new RangeViewDoubleValuesPart(rowId, values,
          (int) matrixParts[i].getStartCol(), (int) matrixParts[i].getEndCol());
    }
    return dataParts;
  }

  /**
   * Split values
   *
   * @param matrixMeta matrix meta
   * @param rowId row id
   * @param values values
   * @return value splits
   */
  public static KeyValuePart[] split(MatrixMeta matrixMeta, int rowId, float[] values) {
    PartitionKey[] matrixParts = matrixMeta.getPartitionKeys();
    KeyValuePart[] dataParts = new KeyValuePart[matrixParts.length];

    for (int i = 0; i < matrixParts.length; i++) {
      dataParts[i] = new RangeViewFloatValuesPart(rowId, values,
          (int) matrixParts[i].getStartCol(), (int) matrixParts[i].getEndCol());
    }
    return dataParts;
  }

  /**
   * Split values
   *
   * @param matrixMeta matrix meta
   * @param rowId row id
   * @param values values
   * @return value splits
   */
  public static KeyValuePart[] split(MatrixMeta matrixMeta, int rowId, int[] values) {
    PartitionKey[] matrixParts = matrixMeta.getPartitionKeys();
    KeyValuePart[] dataParts = new KeyValuePart[matrixParts.length];

    for (int i = 0; i < matrixParts.length; i++) {
      dataParts[i] = new RangeViewIntValuesPart(rowId, values,
          (int) matrixParts[i].getStartCol(), (int) matrixParts[i].getEndCol());
    }
    return dataParts;
  }

  /**
   * Split values
   *
   * @param matrixMeta matrix meta
   * @param rowId row id
   * @param values values
   * @return value splits
   */
  public static KeyValuePart[] split(MatrixMeta matrixMeta, int rowId, long[] values) {
    PartitionKey[] matrixParts = matrixMeta.getPartitionKeys();
    KeyValuePart[] dataParts = new KeyValuePart[matrixParts.length];

    for (int i = 0; i < matrixParts.length; i++) {
      dataParts[i] = new RangeViewLongValuesPart(rowId, values,
          (int) matrixParts[i].getStartCol(), (int) matrixParts[i].getEndCol());
    }
    return dataParts;
  }

  /**
   * Split values
   *
   * @param matrixMeta matrix meta
   * @param rowId row id
   * @param values values
   * @return value splits
   */
  public static KeyValuePart[] split(MatrixMeta matrixMeta, int rowId, IElement[] values) {
    PartitionKey[] matrixParts = matrixMeta.getPartitionKeys();
    KeyValuePart[] dataParts = new KeyValuePart[matrixParts.length];

    for (int i = 0; i < matrixParts.length; i++) {
      dataParts[i] = new RangeViewAnyValuesPart(rowId, values,
          (int) matrixParts[i].getStartCol(), (int) matrixParts[i].getEndCol());
    }
    return dataParts;
  }

  /**
   * Split keys by matrix partition
   *
   * @param matrixMeta matrix meta data
   * @param vector Matrix vector
   * @return partition key to key partition map
   */
  public static KeyValuePart[] split(MatrixMeta matrixMeta, Vector vector) {
    switch (vector.getType()) {
      case T_DOUBLE_SPARSE:
      case T_DOUBLE_DENSE:
        return splitIntDoubleVector(matrixMeta, (IntDoubleVector) vector);

      case T_FLOAT_SPARSE:
      case T_FLOAT_DENSE:
        return splitIntFloatVector(matrixMeta, (IntFloatVector) vector);

      case T_INT_DENSE:
      case T_INT_SPARSE:
        return splitIntIntVector(matrixMeta, (IntIntVector) vector);

      case T_LONG_DENSE:
      case T_LONG_SPARSE:
        return splitIntLongVector(matrixMeta, (IntLongVector) vector);

      case T_DOUBLE_SPARSE_LONGKEY:
        return splitLongDoubleVector(matrixMeta, (LongDoubleVector) vector);

      case T_FLOAT_SPARSE_LONGKEY:
        return splitLongFloatVector(matrixMeta, (LongFloatVector) vector);

      case T_INT_SPARSE_LONGKEY:
        return splitLongIntVector(matrixMeta, (LongIntVector) vector);

      case T_LONG_SPARSE_LONGKEY:
        return splitLongLongVector(matrixMeta, (LongLongVector) vector);


      default:
        throw new UnsupportedOperationException("Unsupport vector type " + vector.getType());
    }
  }


  /**
   * Split keys by matrix partition
   *
   * @param matrixMeta matrix meta data
   * @param vector Matrix vector
   * @return partition key to key partition map
   */
  public static CompStreamKeyValuePart[] splitStream(MatrixMeta matrixMeta, Vector vector) {
    Vector[] vectors = new Vector[1];
    vectors[0] = vector;
    return splitStream(matrixMeta, vectors);
  }

  /**
   * Split keys by matrix partition
   *
   * @param matrixMeta matrix meta data
   * @param vectors Matrix vectors
   * @return partition key to key partition map
   */
  public static CompStreamKeyValuePart[] splitStream(MatrixMeta matrixMeta, Vector[] vectors) {
    CompStreamKeyValuePart[] dataParts = new CompStreamKeyValuePart[matrixMeta.getPartitionNum()];
    KeyValuePart[][] subDataParts = new KeyValuePart[vectors.length][];

    for (int i = 0; i < vectors.length; i++) {
      subDataParts[i] = split(matrixMeta, vectors[i]);
    }

    for (int i = 0; i < dataParts.length; i++) {
      dataParts[i] = new CompStreamKeyValuePart(vectors.length);
      for (int j = 0; j < vectors.length; j++) {
        dataParts[i].add(subDataParts[j][i]);
      }
    }

    return dataParts;
  }

  public static KeyValuePart[] splitIntDoubleVector(MatrixMeta matrixMeta, IntDoubleVector vector) {
    IntDoubleVectorStorage storage = vector.getStorage();
    if (storage.isSparse()) {
      // Get keys and values
      IntDoubleSparseVectorStorage sparseStorage = (IntDoubleSparseVectorStorage) storage;
      int[] keys = sparseStorage.getIndices();
      double[] values = sparseStorage.getValues();
      return split(matrixMeta, vector.getRowId(), keys, values, false);
    } else if (storage.isDense()) {
      // Get values
      IntDoubleDenseVectorStorage denseStorage = (IntDoubleDenseVectorStorage) storage;
      double[] values = denseStorage.getValues();
      return split(matrixMeta, vector.getRowId(), values);
    } else {
      // Key and value array pair
      IntDoubleSortedVectorStorage sortStorage = (IntDoubleSortedVectorStorage) storage;
      int[] keys = sortStorage.getIndices();
      double[] values = sortStorage.getValues();
      return split(matrixMeta, vector.getRowId(), keys, values, true);
    }
  }

  public static KeyValuePart[] splitIntFloatVector(MatrixMeta matrixMeta, IntFloatVector vector) {
    IntFloatVectorStorage storage = vector.getStorage();
    if (storage.isSparse()) {
      // Get keys and values
      IntFloatSparseVectorStorage sparseStorage = (IntFloatSparseVectorStorage) storage;
      int[] keys = sparseStorage.getIndices();
      float[] values = sparseStorage.getValues();
      return split(matrixMeta, vector.getRowId(), keys, values, false);
    } else if (storage.isDense()) {
      // Get values
      IntFloatDenseVectorStorage denseStorage = (IntFloatDenseVectorStorage) storage;
      float[] values = denseStorage.getValues();
      return split(matrixMeta, vector.getRowId(), values);
    } else {
      // Key and value array pair
      IntFloatSortedVectorStorage sortStorage = (IntFloatSortedVectorStorage) storage;
      int[] keys = sortStorage.getIndices();
      float[] values = sortStorage.getValues();
      return split(matrixMeta, vector.getRowId(), keys, values, true);
    }
  }

  public static KeyValuePart[] splitIntIntVector(MatrixMeta matrixMeta, IntIntVector vector) {
    IntIntVectorStorage storage = vector.getStorage();
    if (storage.isSparse()) {
      // Get keys and values
      IntIntSparseVectorStorage sparseStorage = (IntIntSparseVectorStorage) storage;
      int[] keys = sparseStorage.getIndices();
      int[] values = sparseStorage.getValues();
      return split(matrixMeta, vector.getRowId(), keys, values, false);
    } else if (storage.isDense()) {
      // Get values
      IntIntDenseVectorStorage denseStorage = (IntIntDenseVectorStorage) storage;
      int[] values = denseStorage.getValues();
      return split(matrixMeta, vector.getRowId(), values);
    } else {
      // Key and value array pair
      IntIntSortedVectorStorage sortStorage = (IntIntSortedVectorStorage) storage;
      int[] keys = sortStorage.getIndices();
      int[] values = sortStorage.getValues();
      return split(matrixMeta, vector.getRowId(), keys, values, true);
    }
  }

  public static KeyValuePart[] splitIntLongVector(MatrixMeta matrixMeta, IntLongVector vector) {
    IntLongVectorStorage storage = vector.getStorage();
    if (storage.isSparse()) {
      // Get keys and values
      IntLongSparseVectorStorage sparseStorage = (IntLongSparseVectorStorage) storage;
      int[] keys = sparseStorage.getIndices();
      long[] values = sparseStorage.getValues();
      return split(matrixMeta, vector.getRowId(), keys, values, false);
    } else if (storage.isDense()) {
      // Get values
      IntLongDenseVectorStorage denseStorage = (IntLongDenseVectorStorage) storage;
      long[] values = denseStorage.getValues();
      return split(matrixMeta, vector.getRowId(), values);
    } else {
      // Key and value array pair
      IntLongSortedVectorStorage sortStorage = (IntLongSortedVectorStorage) storage;
      int[] keys = sortStorage.getIndices();
      long[] values = sortStorage.getValues();
      return split(matrixMeta, vector.getRowId(), keys, values, true);
    }
  }

  public static KeyValuePart[] splitLongDoubleVector(MatrixMeta matrixMeta, LongDoubleVector vector) {
    LongDoubleVectorStorage storage = vector.getStorage();
    if (storage.isSparse()) {
      // Get keys and values
      LongDoubleSparseVectorStorage sparseStorage = (LongDoubleSparseVectorStorage) storage;
      long[] keys = sparseStorage.getIndices();
      double[] values = sparseStorage.getValues();
      return split(matrixMeta, vector.getRowId(), keys, values, false);
    } else {
      // Key and value array pair
      LongDoubleSortedVectorStorage sortStorage = (LongDoubleSortedVectorStorage) storage;
      long[] keys = sortStorage.getIndices();
      double[] values = sortStorage.getValues();
      return split(matrixMeta, vector.getRowId(), keys, values, true);
    }
  }

  public static KeyValuePart[] splitLongFloatVector(MatrixMeta matrixMeta, LongFloatVector vector) {
    LongFloatVectorStorage storage = vector.getStorage();
    if (storage.isSparse()) {
      // Get keys and values
      LongFloatSparseVectorStorage sparseStorage = (LongFloatSparseVectorStorage) storage;
      long[] keys = sparseStorage.getIndices();
      float[] values = sparseStorage.getValues();
      return split(matrixMeta, vector.getRowId(), keys, values, false);
    } else {
      // Key and value array pair
      LongFloatSortedVectorStorage sortStorage = (LongFloatSortedVectorStorage) storage;
      long[] keys = sortStorage.getIndices();
      float[] values = sortStorage.getValues();
      return split(matrixMeta, vector.getRowId(), keys, values, true);
    }
  }

  public static KeyValuePart[] splitLongIntVector(MatrixMeta matrixMeta, LongIntVector vector) {
    LongIntVectorStorage storage = vector.getStorage();
    if (storage.isSparse()) {
      // Get keys and values
      LongIntSparseVectorStorage sparseStorage = (LongIntSparseVectorStorage) storage;
      long[] keys = sparseStorage.getIndices();
      int[] values = sparseStorage.getValues();
      return split(matrixMeta, vector.getRowId(), keys, values, false);
    } else {
      // Key and value array pair
      LongIntSortedVectorStorage sortStorage = (LongIntSortedVectorStorage) storage;
      long[] keys = sortStorage.getIndices();
      int[] values = sortStorage.getValues();
      return split(matrixMeta, vector.getRowId(), keys, values, true);
    }
  }

  public static KeyValuePart[] splitLongLongVector(MatrixMeta matrixMeta, LongLongVector vector) {
    LongLongVectorStorage storage = vector.getStorage();
    if (storage.isSparse()) {
      // Get keys and values
      LongLongSparseVectorStorage sparseStorage = (LongLongSparseVectorStorage) storage;
      long[] keys = sparseStorage.getIndices();
      long[] values = sparseStorage.getValues();
      return split(matrixMeta, vector.getRowId(), keys, values, false);
    } else {
      // Key and value array pair
      LongLongSortedVectorStorage sortStorage = (LongLongSortedVectorStorage) storage;
      long[] keys = sortStorage.getIndices();
      long[] values = sortStorage.getValues();
      return split(matrixMeta, vector.getRowId(), keys, values, true);
    }
  }
}

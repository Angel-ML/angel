package com.tencent.angel.psagent.matrix.transport.router.hash;

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
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ps.storage.vector.element.IElement;
import com.tencent.angel.psagent.matrix.transport.router.CompStreamKeyValuePart;
import com.tencent.angel.psagent.matrix.transport.router.KeyHash;
import com.tencent.angel.psagent.matrix.transport.router.KeyPart;
import com.tencent.angel.psagent.matrix.transport.router.KeyValuePart;
import it.unimi.dsi.fastutil.ints.Int2DoubleMap;
import it.unimi.dsi.fastutil.ints.Int2FloatMap;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2FloatMap;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

public class HashRouterUtils {

  private static int[] computeHashCode(MatrixMeta matrixMeta, int[] keys) {
    Class<? extends KeyHash> hashClass = matrixMeta.getRouterHash();
    KeyHash hasher = HasherFactory.getHasher(hashClass);
    int[] hashCodes = new int[keys.length];
    for (int i = 0; i < keys.length; i++) {
      hashCodes[i] = computeHashCode(hasher, keys[i]);
    }
    return hashCodes;
  }

  private static int[] computeHashCode(MatrixMeta matrixMeta, long[] keys) {
    Class<? extends KeyHash> hashClass = matrixMeta.getRouterHash();
    KeyHash hasher = HasherFactory.getHasher(hashClass);
    int[] hashCodes = new int[keys.length];
    for (int i = 0; i < keys.length; i++) {
      hashCodes[i] = computeHashCode(hasher, keys[i]);
    }
    return hashCodes;
  }

  private static int[] computeHashCode(MatrixMeta matrixMeta, String[] keys) {
    Class<? extends KeyHash> hashClass = matrixMeta.getRouterHash();
    KeyHash hasher = HasherFactory.getHasher(hashClass);
    int[] hashCodes = new int[keys.length];
    for (int i = 0; i < keys.length; i++) {
      hashCodes[i] = computeHashCode(hasher, keys[i]);
    }
    return hashCodes;
  }

  private static int[] computeHashCode(MatrixMeta matrixMeta, IElement[] keys) {
    Class<? extends KeyHash> hashClass = matrixMeta.getRouterHash();
    KeyHash hasher = HasherFactory.getHasher(hashClass);
    int[] hashCodes = new int[keys.length];
    for (int i = 0; i < keys.length; i++) {
      hashCodes[i] = computeHashCode(hasher, keys[i]);
    }
    return hashCodes;
  }

  private static int computeHashCode(KeyHash hasher, int key) {
    int hashCode = hasher.hash(key);
    hashCode = hashCode ^ (hashCode >>> 16);
    if(hashCode < 0) {
      hashCode -= Integer.MIN_VALUE;
    }
    return hashCode;
  }

  private static int computeHashCode(KeyHash hasher, long key) {
    int hashCode = hasher.hash(key);
    hashCode = hashCode ^ (hashCode >>> 16);
    if (hashCode < 0) {
      hashCode -= Integer.MIN_VALUE;
    }
    return hashCode;
  }

  private static int computeHashCode(KeyHash hasher, String key) {
    int hashCode = hasher.hash(key);
    hashCode = hashCode ^ (hashCode >>> 16);
    if (hashCode < 0) {
      hashCode -= Integer.MIN_VALUE;
    }
    return hashCode;
  }

  private static int computeHashCode(KeyHash hasher, IElement key) {
    int hashCode = hasher.hash(key);
    hashCode = hashCode ^ (hashCode >>> 16);
    if (hashCode < 0) {
      hashCode -= Integer.MIN_VALUE;
    }
    return hashCode;
  }

  /**
   * Split keys by matrix partition
   *
   * @param matrixMeta matrix meta data
   * @param keys the keys need partitioned
   * @return partition key to key partition map
   */
  public static KeyPart[] split(MatrixMeta matrixMeta, int rowId, int[] keys) {
    PartitionKey[] matrixParts = matrixMeta.getPartitionKeys();
    int matrixPartNumMinus1 = matrixParts.length - 1;

    KeyPart[] keyParts = new KeyPart[matrixParts.length];
    int avgPartElemNum = keys.length / matrixParts.length;
    for (int i = 0; i < keyParts.length; i++) {
      keyParts[i] = new HashIntKeysPart(rowId, avgPartElemNum);
    }

    int[] hashCodes = computeHashCode(matrixMeta, keys);

    if (isPow2(matrixParts.length)) {
      for (int i = 0; i < hashCodes.length; i++) {
        int partIndex = hashCodes[i] & (matrixPartNumMinus1);
        ((HashIntKeysPart) keyParts[partIndex]).add(keys[i]);
      }
    } else {
      for (int i = 0; i < hashCodes.length; i++) {
        int partIndex = hashCodes[i] % matrixParts.length;
        ((HashIntKeysPart) keyParts[partIndex]).add(keys[i]);
      }
    }
    return keyParts;
  }

  public static boolean isPow2(int partNum) {
    return (partNum & (partNum - 1)) == 0;
  }

  /**
   * Split keys by matrix partition
   *
   * @param matrixMeta matrix meta data
   * @param keys the keys need partitioned
   * @return partition key to key partition map
   */
  public static KeyPart[] split(MatrixMeta matrixMeta, int rowId, long[] keys) {
    PartitionKey[] matrixParts = matrixMeta.getPartitionKeys();
    int matrixPartNumMinus1 = matrixParts.length - 1;

    KeyPart[] keyParts = new KeyPart[matrixParts.length];
    int avgPartElemNum = keys.length / matrixParts.length;
    for (int i = 0; i < keyParts.length; i++) {
      keyParts[i] = new HashLongKeysPart(rowId, avgPartElemNum);
    }

    int[] hashCodes = computeHashCode(matrixMeta, keys);

    if (isPow2(matrixParts.length)) {
      for (int i = 0; i < hashCodes.length; i++) {
        int partIndex = hashCodes[i] & (matrixPartNumMinus1);
        ((HashLongKeysPart) keyParts[partIndex]).add(keys[i]);
      }
    } else {
      for (int i = 0; i < hashCodes.length; i++) {
        int partIndex = hashCodes[i] % matrixParts.length;
        ((HashLongKeysPart) keyParts[partIndex]).add(keys[i]);
      }
    }

    return keyParts;
  }

  /**
   * Split keys by matrix partition
   *
   * @param matrixMeta matrix meta data
   * @param keys the keys need partitioned
   * @return partition key to key partition map
   */
  public static KeyPart[] split(MatrixMeta matrixMeta, int rowId, String[] keys) {
    PartitionKey[] matrixParts = matrixMeta.getPartitionKeys();
    int matrixPartNumMinus1 = matrixParts.length - 1;

    KeyPart[] keyParts = new KeyPart[matrixParts.length];
    int avgPartElemNum = keys.length / matrixParts.length;
    for (int i = 0; i < keyParts.length; i++) {
      keyParts[i] = new HashStringKeysPart(rowId, avgPartElemNum);
    }

    int[] hashCodes = computeHashCode(matrixMeta, keys);

    if (isPow2(matrixParts.length)) {
      for (int i = 0; i < hashCodes.length; i++) {
        int partIndex = hashCodes[i] & (matrixPartNumMinus1);
        ((HashStringKeysPart) keyParts[partIndex]).add(keys[i]);
      }
    } else {
      for (int i = 0; i < hashCodes.length; i++) {
        int partIndex = hashCodes[i] % matrixParts.length;
        ((HashStringKeysPart) keyParts[partIndex]).add(keys[i]);
      }
    }

    return keyParts;
  }

  /**
   * Split keys by matrix partition
   *
   * @param matrixMeta matrix meta data
   * @param keys the keys need partitioned
   * @return partition key to key partition map
   */
  public static KeyPart[] split(MatrixMeta matrixMeta, int rowId, IElement[] keys) {
    PartitionKey[] matrixParts = matrixMeta.getPartitionKeys();
    int matrixPartNumMinus1 = matrixParts.length - 1;

    KeyPart[] keyParts = new KeyPart[matrixParts.length];
    int avgPartElemNum = keys.length / matrixParts.length;
    for (int i = 0; i < keyParts.length; i++) {
      keyParts[i] = new HashAnyKeysPart(rowId, avgPartElemNum);
    }

    int[] hashCodes = computeHashCode(matrixMeta, keys);

    if (isPow2(matrixParts.length)) {
      for (int i = 0; i < hashCodes.length; i++) {
        int partIndex = hashCodes[i] & (matrixPartNumMinus1);
        ((HashAnyKeysPart) keyParts[partIndex]).add(keys[i]);
      }
    } else {
      for (int i = 0; i < hashCodes.length; i++) {
        int partIndex = hashCodes[i] % matrixParts.length;
        ((HashAnyKeysPart) keyParts[partIndex]).add(keys[i]);
      }
    }

    return keyParts;
  }

  /**
   * Split keys by matrix partition
   *
   * @param matrixMeta matrix meta data
   * @param keys the keys need partitioned
   * @return partition key to key partition map
   */
  public static KeyValuePart[] split(MatrixMeta matrixMeta, int rowId, int[] keys, float[] values) {
    PartitionKey[] matrixParts = matrixMeta.getPartitionKeys();
    int matrixPartNumMinus1 = matrixParts.length - 1;

    KeyValuePart[] keyValueParts = new KeyValuePart[matrixParts.length];
    int avgPartElemNum = keys.length / matrixParts.length;
    for (int i = 0; i < keyValueParts.length; i++) {
      keyValueParts[i] = new HashIntKeysFloatValuesPart(rowId, avgPartElemNum);
    }

    int[] hashCodes = computeHashCode(matrixMeta, keys);

    if (isPow2(matrixParts.length)) {
      for (int i = 0; i < hashCodes.length; i++) {
        int partIndex = hashCodes[i] & (matrixPartNumMinus1);
        ((HashIntKeysFloatValuesPart) keyValueParts[partIndex]).add(keys[i], values[i]);
      }
    } else {
      for (int i = 0; i < hashCodes.length; i++) {
        int partIndex = hashCodes[i] % matrixParts.length;
        ((HashIntKeysFloatValuesPart) keyValueParts[partIndex]).add(keys[i], values[i]);
      }
    }

    return keyValueParts;
  }

  /**
   * Split keys by matrix partition
   *
   * @param matrixMeta matrix meta data
   * @param keys the keys need partitioned
   * @return partition key to key partition map
   */
  public static KeyValuePart[] split(MatrixMeta matrixMeta, int rowId, int[] keys, int[] values) {
    PartitionKey[] matrixParts = matrixMeta.getPartitionKeys();
    int matrixPartNumMinus1 = matrixParts.length - 1;

    KeyValuePart[] keyValueParts = new KeyValuePart[matrixParts.length];
    int avgPartElemNum = keys.length / matrixParts.length;
    for (int i = 0; i < keyValueParts.length; i++) {
      keyValueParts[i] = new HashIntKeysIntValuesPart(rowId, avgPartElemNum);
    }

    int[] hashCodes = computeHashCode(matrixMeta, keys);

    if (isPow2(matrixParts.length)) {
      for (int i = 0; i < hashCodes.length; i++) {
        int partIndex = hashCodes[i] & (matrixPartNumMinus1);
        ((HashIntKeysIntValuesPart) keyValueParts[partIndex]).add(keys[i], values[i]);
      }
    } else {
      for (int i = 0; i < hashCodes.length; i++) {
        int partIndex = hashCodes[i] % matrixParts.length;
        ((HashIntKeysIntValuesPart) keyValueParts[partIndex]).add(keys[i], values[i]);
      }
    }

    return keyValueParts;
  }

  /**
   * Split keys by matrix partition
   *
   * @param matrixMeta matrix meta data
   * @param keys the keys need partitioned
   * @return partition key to key partition map
   */
  public static KeyValuePart[] split(MatrixMeta matrixMeta, int rowId, int[] keys, long[] values) {
    PartitionKey[] matrixParts = matrixMeta.getPartitionKeys();
    int matrixPartNumMinus1 = matrixParts.length - 1;

    KeyValuePart[] keyValueParts = new KeyValuePart[matrixParts.length];
    int avgPartElemNum = keys.length / matrixParts.length;
    for (int i = 0; i < keyValueParts.length; i++) {
      keyValueParts[i] = new HashIntKeysLongValuesPart(rowId, avgPartElemNum);
    }

    int[] hashCodes = computeHashCode(matrixMeta, keys);

    if (isPow2(matrixParts.length)) {
      for (int i = 0; i < hashCodes.length; i++) {
        int partIndex = hashCodes[i] & (matrixPartNumMinus1);
        ((HashIntKeysLongValuesPart) keyValueParts[partIndex]).add(keys[i], values[i]);
      }
    } else {
      for (int i = 0; i < hashCodes.length; i++) {
        int partIndex = hashCodes[i] % matrixParts.length;
        ((HashIntKeysLongValuesPart) keyValueParts[partIndex]).add(keys[i], values[i]);
      }
    }

    return keyValueParts;
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
      float[] values) {
    PartitionKey[] matrixParts = matrixMeta.getPartitionKeys();
    int matrixPartNumMinus1 = matrixParts.length - 1;

    KeyValuePart[] keyValueParts = new KeyValuePart[matrixParts.length];
    int avgPartElemNum = keys.length / matrixParts.length;
    for (int i = 0; i < keyValueParts.length; i++) {
      keyValueParts[i] = new HashLongKeysFloatValuesPart(rowId, avgPartElemNum);
    }

    int[] hashCodes = computeHashCode(matrixMeta, keys);

    if (isPow2(matrixParts.length)) {
      for (int i = 0; i < hashCodes.length; i++) {
        int partIndex = hashCodes[i] & (matrixPartNumMinus1);
        ((HashLongKeysFloatValuesPart) keyValueParts[partIndex]).add(keys[i], values[i]);
      }
    } else {
      for (int i = 0; i < hashCodes.length; i++) {
        int partIndex = hashCodes[i] % matrixParts.length;
        ((HashLongKeysFloatValuesPart) keyValueParts[partIndex]).add(keys[i], values[i]);
      }
    }
    return keyValueParts;
  }

  /**
   * Split keys by matrix partition
   *
   * @param matrixMeta matrix meta data
   * @param keys the keys need partitioned
   * @return partition key to key partition map
   */
  public static KeyValuePart[] split(MatrixMeta matrixMeta, int rowId, int[] keys,
      double[] values) {
    PartitionKey[] matrixParts = matrixMeta.getPartitionKeys();
    int matrixPartNumMinus1 = matrixParts.length - 1;

    KeyValuePart[] keyValueParts = new KeyValuePart[matrixParts.length];
    int avgPartElemNum = keys.length / matrixParts.length;
    for (int i = 0; i < keyValueParts.length; i++) {
      keyValueParts[i] = new HashIntKeysDoubleValuesPart(rowId, avgPartElemNum);
    }

    int[] hashCodes = computeHashCode(matrixMeta, keys);

    if (isPow2(matrixParts.length)) {
      for (int i = 0; i < hashCodes.length; i++) {
        int partIndex = hashCodes[i] & (matrixPartNumMinus1);
        ((HashIntKeysDoubleValuesPart) keyValueParts[partIndex]).add(keys[i], values[i]);
      }
    } else {
      for (int i = 0; i < hashCodes.length; i++) {
        int partIndex = hashCodes[i] % matrixParts.length;
        ((HashIntKeysDoubleValuesPart) keyValueParts[partIndex]).add(keys[i], values[i]);
      }
    }
    return keyValueParts;
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
      double[] values) {
    PartitionKey[] matrixParts = matrixMeta.getPartitionKeys();
    int matrixPartNumMinus1 = matrixParts.length - 1;

    KeyValuePart[] keyValueParts = new KeyValuePart[matrixParts.length];
    int avgPartElemNum = keys.length / matrixParts.length;
    for (int i = 0; i < keyValueParts.length; i++) {
      keyValueParts[i] = new HashLongKeysDoubleValuesPart(rowId, avgPartElemNum);
    }

    int[] hashCodes = computeHashCode(matrixMeta, keys);

    if (isPow2(matrixParts.length)) {
      for (int i = 0; i < hashCodes.length; i++) {
        int partIndex = hashCodes[i] & (matrixPartNumMinus1);
        ((HashLongKeysDoubleValuesPart) keyValueParts[partIndex]).add(keys[i], values[i]);
      }
    } else {
      for (int i = 0; i < hashCodes.length; i++) {
        int partIndex = hashCodes[i] % matrixParts.length;
        ((HashLongKeysDoubleValuesPart) keyValueParts[partIndex]).add(keys[i], values[i]);
      }
    }

    return keyValueParts;
  }

  /**
   * Split keys by matrix partition
   *
   * @param matrixMeta matrix meta data
   * @param keys the keys need partitioned
   * @param values the values of the keys
   * @return partition key to key partition map
   */
  public static KeyValuePart[] split(MatrixMeta matrixMeta, int rowId, long[] keys, int[] values) {
    PartitionKey[] matrixParts = matrixMeta.getPartitionKeys();
    int matrixPartNumMinus1 = matrixParts.length - 1;

    KeyValuePart[] keyValueParts = new KeyValuePart[matrixParts.length];
    int avgPartElemNum = keys.length / matrixParts.length;
    for (int i = 0; i < keyValueParts.length; i++) {
      keyValueParts[i] = new HashLongKeysIntValuesPart(rowId, avgPartElemNum);
    }

    int[] hashCodes = computeHashCode(matrixMeta, keys);

    if (isPow2(matrixParts.length)) {
      for (int i = 0; i < hashCodes.length; i++) {
        int partIndex = hashCodes[i] & (matrixPartNumMinus1);
        ((HashLongKeysIntValuesPart) keyValueParts[partIndex]).add(keys[i], values[i]);
      }
    } else {
      for (int i = 0; i < hashCodes.length; i++) {
        int partIndex = hashCodes[i] % matrixParts.length;
        ((HashLongKeysIntValuesPart) keyValueParts[partIndex]).add(keys[i], values[i]);
      }
    }
    return keyValueParts;
  }

  /**
   * Split keys by matrix partition
   *
   * @param matrixMeta matrix meta data
   * @param keys the keys need partitioned
   * @param values the values of the keys
   * @return partition key to key partition map
   */
  public static KeyValuePart[] split(MatrixMeta matrixMeta, int rowId, long[] keys, long[] values) {
    PartitionKey[] matrixParts = matrixMeta.getPartitionKeys();
    int matrixPartNumMinus1 = matrixParts.length - 1;

    KeyValuePart[] keyValueParts = new KeyValuePart[matrixParts.length];
    int avgPartElemNum = keys.length / matrixParts.length;
    for (int i = 0; i < keyValueParts.length; i++) {
      keyValueParts[i] = new HashLongKeysLongValuesPart(rowId, avgPartElemNum);
    }

    int[] hashCodes = computeHashCode(matrixMeta, keys);

    if (isPow2(matrixParts.length)) {
      for (int i = 0; i < hashCodes.length; i++) {
        int partIndex = hashCodes[i] & (matrixPartNumMinus1);
        ((HashLongKeysLongValuesPart) keyValueParts[partIndex]).add(keys[i], values[i]);
      }
    } else {
      for (int i = 0; i < hashCodes.length; i++) {
        int partIndex = hashCodes[i] % matrixParts.length;
        ((HashLongKeysLongValuesPart) keyValueParts[partIndex]).add(keys[i], values[i]);
      }
    }
    return keyValueParts;
  }

  /**
   * Split keys by matrix partition
   *
   * @param matrixMeta matrix meta data
   * @param keys the keys need partitioned
   * @return partition key to key partition map
   */
  public static KeyValuePart[] split(MatrixMeta matrixMeta, int rowId, int[] keys,
      IElement[] values) {
    PartitionKey[] matrixParts = matrixMeta.getPartitionKeys();
    int matrixPartNumMinus1 = matrixParts.length - 1;

    KeyValuePart[] keyValueParts = new KeyValuePart[matrixParts.length];
    int avgPartElemNum = keys.length / matrixParts.length;
    for (int i = 0; i < keyValueParts.length; i++) {
      keyValueParts[i] = new HashIntKeysAnyValuesPart(rowId, avgPartElemNum);
    }

    int[] hashCodes = computeHashCode(matrixMeta, keys);

    if (isPow2(matrixParts.length)) {
      for (int i = 0; i < hashCodes.length; i++) {
        int partIndex = hashCodes[i] & (matrixPartNumMinus1);
        ((HashIntKeysAnyValuesPart) keyValueParts[partIndex]).add(keys[i], values[i]);
      }
    } else {
      for (int i = 0; i < hashCodes.length; i++) {
        int partIndex = hashCodes[i] % matrixParts.length;
        ((HashIntKeysAnyValuesPart) keyValueParts[partIndex]).add(keys[i], values[i]);
      }
    }

    return keyValueParts;
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
      IElement[] values) {
    PartitionKey[] matrixParts = matrixMeta.getPartitionKeys();
    int matrixPartNumMinus1 = matrixParts.length - 1;

    KeyValuePart[] keyValueParts = new KeyValuePart[matrixParts.length];
    int avgPartElemNum = keys.length / matrixParts.length;
    for (int i = 0; i < keyValueParts.length; i++) {
      keyValueParts[i] = new HashLongKeysAnyValuesPart(rowId, avgPartElemNum);
    }

    int[] hashCodes = computeHashCode(matrixMeta, keys);

    if (isPow2(matrixParts.length)) {
      for (int i = 0; i < hashCodes.length; i++) {
        int partIndex = hashCodes[i] & (matrixPartNumMinus1);
        ((HashLongKeysAnyValuesPart) keyValueParts[partIndex]).add(keys[i], values[i]);
      }
    } else {
      for (int i = 0; i < hashCodes.length; i++) {
        int partIndex = hashCodes[i] % matrixParts.length;
        ((HashLongKeysAnyValuesPart) keyValueParts[partIndex]).add(keys[i], values[i]);
      }
    }
    return keyValueParts;
  }

  /**
   * Split keys by matrix partition
   *
   * @param matrixMeta matrix meta data
   * @param keys the keys need partitioned
   * @param values the values of the keys
   * @return partition key to key partition map
   */
  public static KeyValuePart[] split(MatrixMeta matrixMeta, int rowId, String[] keys,
      IElement[] values) {
    PartitionKey[] matrixParts = matrixMeta.getPartitionKeys();
    int matrixPartNumMinus1 = matrixParts.length - 1;

    KeyValuePart[] keyValueParts = new KeyValuePart[matrixParts.length];
    int avgPartElemNum = keys.length / matrixParts.length;
    for (int i = 0; i < keyValueParts.length; i++) {
      keyValueParts[i] = new HashStringKeysAnyValuesPart(rowId, avgPartElemNum);
    }

    int[] hashCodes = computeHashCode(matrixMeta, keys);

    if (isPow2(matrixParts.length)) {
      for (int i = 0; i < hashCodes.length; i++) {
        int partIndex = hashCodes[i] & (matrixPartNumMinus1);
        ((HashStringKeysAnyValuesPart) keyValueParts[partIndex]).add(keys[i], values[i]);
      }
    } else {
      for (int i = 0; i < hashCodes.length; i++) {
        int partIndex = hashCodes[i] % matrixParts.length;
        ((HashStringKeysAnyValuesPart) keyValueParts[partIndex]).add(keys[i], values[i]);
      }
    }

    return keyValueParts;
  }

  /**
   * Split keys by matrix partition
   *
   * @param matrixMeta matrix meta data
   * @param keys the keys need partitioned
   * @param values the values of the keys
   * @return partition key to key partition map
   */
  public static KeyValuePart[] split(MatrixMeta matrixMeta, int rowId, IElement[] keys,
      IElement[] values) {
    PartitionKey[] matrixParts = matrixMeta.getPartitionKeys();
    int matrixPartNumMinus1 = matrixParts.length - 1;

    KeyValuePart[] keyValueParts = new KeyValuePart[matrixParts.length];
    int avgPartElemNum = keys.length / matrixParts.length;
    for (int i = 0; i < keyValueParts.length; i++) {
      keyValueParts[i] = new HashAnyKeysAnyValuesPart(rowId, avgPartElemNum);
    }

    int[] hashCodes = computeHashCode(matrixMeta, keys);

    if (isPow2(matrixParts.length)) {
      for (int i = 0; i < hashCodes.length; i++) {
        int partIndex = hashCodes[i] & (matrixPartNumMinus1);
        ((HashAnyKeysAnyValuesPart) keyValueParts[partIndex]).add(keys[i], values[i]);
      }
    } else {
      for (int i = 0; i < hashCodes.length; i++) {
        int partIndex = hashCodes[i] % matrixParts.length;
        ((HashAnyKeysAnyValuesPart) keyValueParts[partIndex]).add(keys[i], values[i]);
      }
    }

    return keyValueParts;
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
    PartitionKey[] matrixParts = matrixMeta.getPartitionKeys();

    // Use comp key value part
    CompStreamKeyValuePart[] dataParts = new CompStreamKeyValuePart[matrixParts.length];
    KeyValuePart[][] subDataParts = new KeyValuePart[vectors.length][];

    // Split each vector
    for (int i = 0; i < subDataParts.length; i++) {
      subDataParts[i] = split(matrixMeta, vectors[i]);
    }

    // Combine sub data part
    for (int i = 0; i < dataParts.length; i++) {
      dataParts[i] = new CompStreamKeyValuePart(vectors.length);
      for (int j = 0; j < vectors.length; j++) {
        dataParts[i].add(subDataParts[j][i]);
      }
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
    KeyHash hasher = HasherFactory.getHasher(matrixMeta.getRouterHash());
    PartitionKey[] matrixParts = matrixMeta.getPartitionKeys();
    KeyValuePart[] dataParts = new KeyValuePart[matrixParts.length];

    int estSize = (int) (vector.getSize() / matrixMeta.getPartitionNum());
    for (int i = 0; i < dataParts.length; i++) {
      dataParts[i] = generateDataPart(vector.getRowId(), vector.getType(), estSize);
    }

    switch (vector.getType()) {
      case T_DOUBLE_DENSE:
      case T_DOUBLE_SPARSE: {
        splitIntDoubleVector(hasher, matrixMeta, (IntDoubleVector) vector, dataParts);
        break;
      }

      case T_FLOAT_DENSE:
      case T_FLOAT_SPARSE: {
        splitIntFloatVector(hasher, matrixMeta, (IntFloatVector) vector, dataParts);
        break;
      }

      case T_INT_DENSE:
      case T_INT_SPARSE: {
        splitIntIntVector(hasher, matrixMeta, (IntIntVector) vector, dataParts);
        break;
      }

      case T_LONG_DENSE:
      case T_LONG_SPARSE: {
        splitIntLongVector(hasher, matrixMeta, (IntLongVector) vector, dataParts);
        break;
      }

      case T_DOUBLE_SPARSE_LONGKEY: {
        splitLongDoubleVector(hasher, matrixMeta, (LongDoubleVector) vector, dataParts);
        break;
      }

      case T_FLOAT_SPARSE_LONGKEY: {
        splitLongFloatVector(hasher, matrixMeta, (LongFloatVector) vector, dataParts);
        break;
      }

      case T_INT_SPARSE_LONGKEY: {
        splitLongIntVector(hasher, matrixMeta, (LongIntVector) vector, dataParts);
        break;
      }

      case T_LONG_SPARSE_LONGKEY: {
        splitLongLongVector(hasher, matrixMeta, (LongLongVector) vector, dataParts);
        break;
      }

      default: {
        throw new UnsupportedOperationException("Unsupport vector type " + vector.getType());
      }

    }

    for (int i = 0; i < dataParts.length; i++) {
      if (dataParts[i] != null) {
        dataParts[i].setRowId(vector.getRowId());
      }
    }
    return dataParts;
  }

  public static void splitIntDoubleVector(KeyHash hasher, MatrixMeta matrixMeta,
      IntDoubleVector vector, KeyValuePart[] dataParts) {
    int dataPartNum = dataParts.length;
    int dataPartNumMinus1 = dataPartNum - 1;
    if (isPow2(dataPartNum)) {
      IntDoubleVectorStorage storage = vector.getStorage();
      if (storage.isSparse()) {
        // Use iterator
        IntDoubleSparseVectorStorage sparseStorage = (IntDoubleSparseVectorStorage) storage;
        ObjectIterator<Int2DoubleMap.Entry> iter = sparseStorage.entryIterator();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry keyValue = iter.next();
          int partId = computeHashCode(hasher, keyValue.getIntKey()) & dataPartNumMinus1;
          ((HashIntKeysDoubleValuesPart) dataParts[partId])
              .add(keyValue.getIntKey(), keyValue.getDoubleValue());
        }
      } else if (storage.isDense()) {
        // Get values
        IntDoubleDenseVectorStorage denseStorage = (IntDoubleDenseVectorStorage) storage;
        double[] values = denseStorage.getValues();
        for (int i = 0; i < values.length; i++) {
          int partId = computeHashCode(hasher, i) & dataPartNumMinus1;
          ((HashIntKeysDoubleValuesPart) dataParts[partId]).add(i, values[i]);
        }
      } else {
        // Key and value array pair
        IntDoubleSortedVectorStorage sortStorage = (IntDoubleSortedVectorStorage) storage;
        int[] keys = sortStorage.getIndices();
        double[] values = sortStorage.getValues();
        for (int i = 0; i < keys.length; i++) {
          int partId = computeHashCode(hasher, keys[i]) & dataPartNumMinus1;
          ((HashIntKeysDoubleValuesPart) dataParts[partId]).add(keys[i], values[i]);
        }
      }
    } else {
      IntDoubleVectorStorage storage = vector.getStorage();
      if (storage.isSparse()) {
        // Use iterator
        IntDoubleSparseVectorStorage sparseStorage = (IntDoubleSparseVectorStorage) storage;
        ObjectIterator<Int2DoubleMap.Entry> iter = sparseStorage.entryIterator();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry keyValue = iter.next();
          int partId = computeHashCode(hasher, keyValue.getIntKey()) % dataPartNum;
          ((HashIntKeysDoubleValuesPart) dataParts[partId])
              .add(keyValue.getIntKey(), keyValue.getDoubleValue());
        }
      } else if (storage.isDense()) {
        // Get values
        IntDoubleDenseVectorStorage denseStorage = (IntDoubleDenseVectorStorage) storage;
        double[] values = denseStorage.getValues();
        for (int i = 0; i < values.length; i++) {
          int partId = computeHashCode(hasher, i) % dataPartNum;
          ((HashIntKeysDoubleValuesPart) dataParts[partId]).add(i, values[i]);
        }
      } else {
        // Key and value array pair
        IntDoubleSortedVectorStorage sortStorage = (IntDoubleSortedVectorStorage) storage;
        int[] keys = sortStorage.getIndices();
        double[] values = sortStorage.getValues();
        for (int i = 0; i < keys.length; i++) {
          int partId = computeHashCode(hasher, keys[i]) % dataPartNum;
          ((HashIntKeysDoubleValuesPart) dataParts[partId]).add(keys[i], values[i]);
        }
      }
    }
  }

  public static void splitIntFloatVector(KeyHash hasher, MatrixMeta matrixMeta,
      IntFloatVector vector, KeyValuePart[] dataParts) {
    int dataPartNum = dataParts.length;
    int dataPartNumMinus1 = dataPartNum - 1;
    if (isPow2(dataPartNum)) {
      IntFloatVectorStorage storage = vector.getStorage();
      if (storage.isSparse()) {
        // Use iterator
        IntFloatSparseVectorStorage sparseStorage = (IntFloatSparseVectorStorage) storage;
        ObjectIterator<Int2FloatMap.Entry> iter = sparseStorage.entryIterator();
        while (iter.hasNext()) {
          Int2FloatMap.Entry keyValue = iter.next();
          int partId = computeHashCode(hasher, keyValue.getIntKey()) & dataPartNumMinus1;
          ((HashIntKeysFloatValuesPart) dataParts[partId])
              .add(keyValue.getIntKey(), keyValue.getFloatValue());
        }
      } else if (storage.isDense()) {
        // Get values
        IntFloatDenseVectorStorage denseStorage = (IntFloatDenseVectorStorage) storage;
        float[] values = denseStorage.getValues();
        for (int i = 0; i < values.length; i++) {
          int partId = computeHashCode(hasher, i) & dataPartNumMinus1;
          ((HashIntKeysFloatValuesPart) dataParts[partId]).add(i, values[i]);
        }
      } else {
        // Key and value array pair
        IntFloatSortedVectorStorage sortStorage = (IntFloatSortedVectorStorage) storage;
        int[] keys = sortStorage.getIndices();
        float[] values = sortStorage.getValues();
        for (int i = 0; i < keys.length; i++) {
          int partId = computeHashCode(hasher, keys[i]) & dataPartNumMinus1;
          ((HashIntKeysFloatValuesPart) dataParts[partId]).add(keys[i], values[i]);
        }
      }
    } else {
      IntFloatVectorStorage storage = vector.getStorage();
      if (storage.isSparse()) {
        // Use iterator
        IntFloatSparseVectorStorage sparseStorage = (IntFloatSparseVectorStorage) storage;
        ObjectIterator<Int2FloatMap.Entry> iter = sparseStorage.entryIterator();
        while (iter.hasNext()) {
          Int2FloatMap.Entry keyValue = iter.next();
          int partId = computeHashCode(hasher, keyValue.getIntKey()) % dataPartNum;
          ((HashIntKeysFloatValuesPart) dataParts[partId])
              .add(keyValue.getIntKey(), keyValue.getFloatValue());
        }
      } else if (storage.isDense()) {
        // Get values
        IntFloatDenseVectorStorage denseStorage = (IntFloatDenseVectorStorage) storage;
        float[] values = denseStorage.getValues();
        for (int i = 0; i < values.length; i++) {
          int partId = computeHashCode(hasher, i) % dataPartNum;
          ((HashIntKeysFloatValuesPart) dataParts[partId]).add(i, values[i]);
        }
      } else {
        // Key and value array pair
        IntFloatSortedVectorStorage sortStorage = (IntFloatSortedVectorStorage) storage;
        int[] keys = sortStorage.getIndices();
        float[] values = sortStorage.getValues();
        for (int i = 0; i < keys.length; i++) {
          int partId = computeHashCode(hasher, keys[i]) % dataPartNum;
          ((HashIntKeysFloatValuesPart) dataParts[partId]).add(keys[i], values[i]);
        }
      }
    }
  }

  public static void splitIntIntVector(KeyHash hasher, MatrixMeta matrixMeta, IntIntVector vector,
      KeyValuePart[] dataParts) {
    int dataPartNum = dataParts.length;
    int dataPartNumMinus1 = dataPartNum - 1;
    if (isPow2(dataPartNum)) {
      IntIntVectorStorage storage = vector.getStorage();
      if (storage.isSparse()) {
        // Use iterator
        IntIntSparseVectorStorage sparseStorage = (IntIntSparseVectorStorage) storage;
        ObjectIterator<Int2IntMap.Entry> iter = sparseStorage.entryIterator();
        while (iter.hasNext()) {
          Int2IntMap.Entry keyValue = iter.next();
          int partId = computeHashCode(hasher, keyValue.getIntKey()) & dataPartNumMinus1;
          ((HashIntKeysIntValuesPart) dataParts[partId])
              .add(keyValue.getIntKey(), keyValue.getIntValue());
        }
      } else if (storage.isDense()) {
        // Get values
        IntIntDenseVectorStorage denseStorage = (IntIntDenseVectorStorage) storage;
        int[] values = denseStorage.getValues();
        for (int i = 0; i < values.length; i++) {
          int partId = computeHashCode(hasher, i) & dataPartNumMinus1;
          ((HashIntKeysIntValuesPart) dataParts[partId]).add(i, values[i]);
        }
      } else {
        // Key and value array pair
        IntIntSortedVectorStorage sortStorage = (IntIntSortedVectorStorage) storage;
        int[] keys = sortStorage.getIndices();
        int[] values = sortStorage.getValues();
        for (int i = 0; i < keys.length; i++) {
          int partId = computeHashCode(hasher, keys[i]) & dataPartNumMinus1;
          ((HashIntKeysIntValuesPart) dataParts[partId]).add(keys[i], values[i]);
        }
      }
    } else {
      IntIntVectorStorage storage = vector.getStorage();
      if (storage.isSparse()) {
        // Use iterator
        IntIntSparseVectorStorage sparseStorage = (IntIntSparseVectorStorage) storage;
        ObjectIterator<Int2IntMap.Entry> iter = sparseStorage.entryIterator();
        while (iter.hasNext()) {
          Int2IntMap.Entry keyValue = iter.next();
          int partId = computeHashCode(hasher, keyValue.getIntKey()) % dataPartNum;
          ((HashIntKeysIntValuesPart) dataParts[partId])
              .add(keyValue.getIntKey(), keyValue.getIntValue());
        }
      } else if (storage.isDense()) {
        // Get values
        IntIntDenseVectorStorage denseStorage = (IntIntDenseVectorStorage) storage;
        int[] values = denseStorage.getValues();
        for (int i = 0; i < values.length; i++) {
          int partId = computeHashCode(hasher, i) % dataPartNum;
          ((HashIntKeysIntValuesPart) dataParts[partId]).add(i, values[i]);
        }
      } else {
        // Key and value array pair
        IntIntSortedVectorStorage sortStorage = (IntIntSortedVectorStorage) storage;
        int[] keys = sortStorage.getIndices();
        int[] values = sortStorage.getValues();
        for (int i = 0; i < keys.length; i++) {
          int partId = computeHashCode(hasher, keys[i]) % dataPartNum;
          ((HashIntKeysIntValuesPart) dataParts[partId]).add(keys[i], values[i]);
        }
      }
    }
  }

  public static void splitIntLongVector(KeyHash hasher, MatrixMeta matrixMeta, IntLongVector vector,
      KeyValuePart[] dataParts) {
    int dataPartNum = dataParts.length;
    int dataPartNumMinus1 = dataPartNum - 1;
    if (isPow2(dataPartNum)) {
      IntLongVectorStorage storage = vector.getStorage();
      if (storage.isSparse()) {
        // Use iterator
        IntLongSparseVectorStorage sparseStorage = (IntLongSparseVectorStorage) storage;
        ObjectIterator<Int2LongMap.Entry> iter = sparseStorage.entryIterator();
        while (iter.hasNext()) {
          Int2LongMap.Entry keyValue = iter.next();
          int partId = computeHashCode(hasher, keyValue.getIntKey()) & dataPartNumMinus1;
          ((HashIntKeysLongValuesPart) dataParts[partId])
              .add(keyValue.getIntKey(), keyValue.getLongValue());
        }
      } else if (storage.isDense()) {
        // Get values
        IntLongDenseVectorStorage denseStorage = (IntLongDenseVectorStorage) storage;
        long[] values = denseStorage.getValues();
        for (int i = 0; i < values.length; i++) {
          int partId = computeHashCode(hasher, i) & dataPartNumMinus1;
          ((HashIntKeysLongValuesPart) dataParts[partId]).add(i, values[i]);
        }
      } else {
        // Key and value array pair
        IntLongSortedVectorStorage sortStorage = (IntLongSortedVectorStorage) storage;
        int[] keys = sortStorage.getIndices();
        long[] values = sortStorage.getValues();
        for (int i = 0; i < keys.length; i++) {
          int partId = computeHashCode(hasher, keys[i]) & dataPartNumMinus1;
          ((HashIntKeysLongValuesPart) dataParts[partId]).add(keys[i], values[i]);
        }
      }
    } else {
      IntLongVectorStorage storage = vector.getStorage();
      if (storage.isSparse()) {
        // Use iterator
        IntLongSparseVectorStorage sparseStorage = (IntLongSparseVectorStorage) storage;
        ObjectIterator<Int2LongMap.Entry> iter = sparseStorage.entryIterator();
        while (iter.hasNext()) {
          Int2LongMap.Entry keyValue = iter.next();
          int partId = computeHashCode(hasher, keyValue.getIntKey()) % dataPartNum;
          ((HashIntKeysLongValuesPart) dataParts[partId])
              .add(keyValue.getIntKey(), keyValue.getLongValue());
        }
      } else if (storage.isDense()) {
        // Get values
        IntLongDenseVectorStorage denseStorage = (IntLongDenseVectorStorage) storage;
        long[] values = denseStorage.getValues();
        for (int i = 0; i < values.length; i++) {
          int partId = computeHashCode(hasher, i) % dataPartNum;
          ((HashIntKeysLongValuesPart) dataParts[partId]).add(i, values[i]);
        }
      } else {
        // Key and value array pair
        IntLongSortedVectorStorage sortStorage = (IntLongSortedVectorStorage) storage;
        int[] keys = sortStorage.getIndices();
        long[] values = sortStorage.getValues();
        for (int i = 0; i < keys.length; i++) {
          int partId = computeHashCode(hasher, keys[i]) % dataPartNum;
          ((HashIntKeysLongValuesPart) dataParts[partId]).add(keys[i], values[i]);
        }
      }
    }
  }

  public static void splitLongDoubleVector(KeyHash hasher, MatrixMeta matrixMeta,
      LongDoubleVector vector, KeyValuePart[] dataParts) {
    int dataPartNum = dataParts.length;
    int dataPartNumMinus1 = dataPartNum - 1;
    if (isPow2(dataPartNum)) {
      LongDoubleVectorStorage storage = vector.getStorage();
      if (storage.isSparse()) {
        // Use iterator
        LongDoubleSparseVectorStorage sparseStorage = (LongDoubleSparseVectorStorage) storage;
        ObjectIterator<Long2DoubleMap.Entry> iter = sparseStorage.entryIterator();
        while (iter.hasNext()) {
          Long2DoubleMap.Entry keyValue = iter.next();
          int partId = computeHashCode(hasher, keyValue.getLongKey()) & dataPartNumMinus1;
          ((HashLongKeysDoubleValuesPart) dataParts[partId])
              .add(keyValue.getLongKey(), keyValue.getDoubleValue());
        }
      } else {
        // Key and value array pair
        LongDoubleSortedVectorStorage sortStorage = (LongDoubleSortedVectorStorage) storage;
        long[] keys = sortStorage.getIndices();
        double[] values = sortStorage.getValues();
        for (int i = 0; i < keys.length; i++) {
          int partId = computeHashCode(hasher, keys[i]) & dataPartNumMinus1;
          ((HashLongKeysDoubleValuesPart) dataParts[partId]).add(keys[i], values[i]);
        }
      }
    } else {
      LongDoubleVectorStorage storage = vector.getStorage();
      if (storage.isSparse()) {
        // Use iterator
        LongDoubleSparseVectorStorage sparseStorage = (LongDoubleSparseVectorStorage) storage;
        ObjectIterator<Long2DoubleMap.Entry> iter = sparseStorage.entryIterator();
        while (iter.hasNext()) {
          Long2DoubleMap.Entry keyValue = iter.next();
          int partId = computeHashCode(hasher, keyValue.getLongKey()) % dataPartNum;
          ((HashLongKeysDoubleValuesPart) dataParts[partId])
              .add(keyValue.getLongKey(), keyValue.getDoubleValue());
        }
      } else {
        // Key and value array pair
        LongDoubleSortedVectorStorage sortStorage = (LongDoubleSortedVectorStorage) storage;
        long[] keys = sortStorage.getIndices();
        double[] values = sortStorage.getValues();
        for (int i = 0; i < keys.length; i++) {
          int partId = computeHashCode(hasher, keys[i]) % dataPartNum;
          ((HashLongKeysDoubleValuesPart) dataParts[partId]).add(keys[i], values[i]);
        }
      }
    }
  }

  public static void splitLongFloatVector(KeyHash hasher, MatrixMeta matrixMeta,
      LongFloatVector vector, KeyValuePart[] dataParts) {
    int dataPartNum = dataParts.length;
    int dataPartNumMinus1 = dataPartNum - 1;
    if (isPow2(dataPartNum)) {
      LongFloatVectorStorage storage = vector.getStorage();
      if (storage.isSparse()) {
        // Use iterator
        LongFloatSparseVectorStorage sparseStorage = (LongFloatSparseVectorStorage) storage;
        ObjectIterator<Long2FloatMap.Entry> iter = sparseStorage.entryIterator();
        while (iter.hasNext()) {
          Long2FloatMap.Entry keyValue = iter.next();
          int partId = computeHashCode(hasher, keyValue.getLongKey()) & dataPartNumMinus1;
          ((HashLongKeysFloatValuesPart) dataParts[partId])
              .add(keyValue.getLongKey(), keyValue.getFloatValue());
        }
      } else {
        // Key and value array pair
        LongFloatSortedVectorStorage sortStorage = (LongFloatSortedVectorStorage) storage;
        long[] keys = sortStorage.getIndices();
        float[] values = sortStorage.getValues();
        for (int i = 0; i < keys.length; i++) {
          int partId = computeHashCode(hasher, keys[i]) & dataPartNumMinus1;
          ((HashLongKeysFloatValuesPart) dataParts[partId]).add(keys[i], values[i]);
        }
      }
    } else {
      LongFloatVectorStorage storage = vector.getStorage();
      if (storage.isSparse()) {
        // Use iterator
        LongFloatSparseVectorStorage sparseStorage = (LongFloatSparseVectorStorage) storage;
        ObjectIterator<Long2FloatMap.Entry> iter = sparseStorage.entryIterator();
        while (iter.hasNext()) {
          Long2FloatMap.Entry keyValue = iter.next();
          int partId = computeHashCode(hasher, keyValue.getLongKey()) % dataPartNum;
          ((HashLongKeysFloatValuesPart) dataParts[partId])
              .add(keyValue.getLongKey(), keyValue.getFloatValue());
        }
      } else {
        // Key and value array pair
        LongFloatSortedVectorStorage sortStorage = (LongFloatSortedVectorStorage) storage;
        long[] keys = sortStorage.getIndices();
        float[] values = sortStorage.getValues();
        for (int i = 0; i < keys.length; i++) {
          int partId = computeHashCode(hasher, keys[i]) % dataPartNum;
          ((HashLongKeysFloatValuesPart) dataParts[partId]).add(keys[i], values[i]);
        }
      }
    }
  }

  public static void splitLongIntVector(KeyHash hasher, MatrixMeta matrixMeta, LongIntVector vector,
      KeyValuePart[] dataParts) {
    int dataPartNum = dataParts.length;
    int dataPartNumMinus1 = dataPartNum - 1;
    if (isPow2(dataPartNum)) {
      LongIntVectorStorage storage = vector.getStorage();
      if (storage.isSparse()) {
        // Use iterator
        LongIntSparseVectorStorage sparseStorage = (LongIntSparseVectorStorage) storage;
        ObjectIterator<Long2IntMap.Entry> iter = sparseStorage.entryIterator();
        while (iter.hasNext()) {
          Long2IntMap.Entry keyValue = iter.next();
          int partId = computeHashCode(hasher, keyValue.getLongKey()) & dataPartNumMinus1;
          ((HashLongKeysIntValuesPart) dataParts[partId])
              .add(keyValue.getLongKey(), keyValue.getIntValue());
        }
      } else {
        // Key and value array pair
        LongIntSortedVectorStorage sortStorage = (LongIntSortedVectorStorage) storage;
        long[] keys = sortStorage.getIndices();
        int[] values = sortStorage.getValues();
        for (int i = 0; i < keys.length; i++) {
          int partId = computeHashCode(hasher, keys[i]) & dataPartNumMinus1;
          ((HashLongKeysIntValuesPart) dataParts[partId]).add(keys[i], values[i]);
        }
      }
    } else {
      LongIntVectorStorage storage = vector.getStorage();
      if (storage.isSparse()) {
        // Use iterator
        LongIntSparseVectorStorage sparseStorage = (LongIntSparseVectorStorage) storage;
        ObjectIterator<Long2IntMap.Entry> iter = sparseStorage.entryIterator();
        while (iter.hasNext()) {
          Long2IntMap.Entry keyValue = iter.next();
          int partId = computeHashCode(hasher, keyValue.getLongKey()) % dataPartNum;
          ((HashLongKeysIntValuesPart) dataParts[partId])
              .add(keyValue.getLongKey(), keyValue.getIntValue());
        }
      } else {
        // Key and value array pair
        LongIntSortedVectorStorage sortStorage = (LongIntSortedVectorStorage) storage;
        long[] keys = sortStorage.getIndices();
        int[] values = sortStorage.getValues();
        for (int i = 0; i < keys.length; i++) {
          int partId = computeHashCode(hasher, keys[i]) % dataPartNum;
          ((HashLongKeysIntValuesPart) dataParts[partId]).add(keys[i], values[i]);
        }
      }
    }
  }

  public static void splitLongLongVector(KeyHash hasher, MatrixMeta matrixMeta,
      LongLongVector vector, KeyValuePart[] dataParts) {
    int dataPartNum = dataParts.length;
    int dataPartNumMinus1 = dataPartNum - 1;
    if (isPow2(dataPartNum)) {
      LongLongVectorStorage storage = vector.getStorage();
      if (storage.isSparse()) {
        // Use iterator
        LongLongSparseVectorStorage sparseStorage = (LongLongSparseVectorStorage) storage;
        ObjectIterator<Long2LongMap.Entry> iter = sparseStorage.entryIterator();
        while (iter.hasNext()) {
          Long2LongMap.Entry keyValue = iter.next();
          int partId = computeHashCode(hasher, keyValue.getLongKey()) & dataPartNumMinus1;
          ((HashLongKeysLongValuesPart) dataParts[partId])
              .add(keyValue.getLongKey(), keyValue.getLongValue());
        }
      } else {
        // Key and value array pair
        LongLongSortedVectorStorage sortStorage = (LongLongSortedVectorStorage) storage;
        long[] keys = sortStorage.getIndices();
        long[] values = sortStorage.getValues();
        for (int i = 0; i < keys.length; i++) {
          int partId = computeHashCode(hasher, keys[i]) & dataPartNumMinus1;
          ((HashLongKeysLongValuesPart) dataParts[partId]).add(keys[i], values[i]);
        }
      }
    } else {
      LongLongVectorStorage storage = vector.getStorage();
      if (storage.isSparse()) {
        // Use iterator
        LongLongSparseVectorStorage sparseStorage = (LongLongSparseVectorStorage) storage;
        ObjectIterator<Long2LongMap.Entry> iter = sparseStorage.entryIterator();
        while (iter.hasNext()) {
          Long2LongMap.Entry keyValue = iter.next();
          int partId = computeHashCode(hasher, keyValue.getLongKey()) % dataPartNum;
          ((HashLongKeysLongValuesPart) dataParts[partId])
              .add(keyValue.getLongKey(), keyValue.getLongValue());
        }
      } else {
        // Key and value array pair
        LongLongSortedVectorStorage sortStorage = (LongLongSortedVectorStorage) storage;
        long[] keys = sortStorage.getIndices();
        long[] values = sortStorage.getValues();
        for (int i = 0; i < keys.length; i++) {
          int partId = computeHashCode(hasher, keys[i]) % dataPartNum;
          ((HashLongKeysLongValuesPart) dataParts[partId]).add(keys[i], values[i]);
        }
      }
    }
  }

  public static KeyValuePart generateDataPart(int rowId, RowType type, int estSize) {
    switch (type) {
      case T_DOUBLE_SPARSE:
      case T_DOUBLE_DENSE: {
        return new HashIntKeysDoubleValuesPart(rowId, estSize);
      }

      case T_FLOAT_DENSE:
      case T_FLOAT_SPARSE: {
        return new HashIntKeysFloatValuesPart(rowId, estSize);
      }

      case T_INT_DENSE:
      case T_INT_SPARSE: {
        return new HashIntKeysIntValuesPart(rowId, estSize);
      }

      case T_LONG_DENSE:
      case T_LONG_SPARSE: {
        return new HashIntKeysLongValuesPart(rowId, estSize);
      }

      case T_DOUBLE_SPARSE_LONGKEY: {
        return new HashLongKeysDoubleValuesPart(rowId, estSize);
      }

      case T_FLOAT_SPARSE_LONGKEY: {
        return new HashLongKeysFloatValuesPart(rowId, estSize);
      }

      case T_INT_SPARSE_LONGKEY: {
        return new HashLongKeysIntValuesPart(rowId, estSize);
      }

      case T_LONG_SPARSE_LONGKEY: {
        return new HashLongKeysLongValuesPart(rowId, estSize);
      }

      default: {
        throw new UnsupportedOperationException("Unsupport vector type " + type);
      }
    }
  }
}

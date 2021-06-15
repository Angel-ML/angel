package com.tencent.angel.psagent.matrix.transport.router;

import com.tencent.angel.ml.math2.matrix.Matrix;
import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ps.storage.vector.element.IElement;
import com.tencent.angel.psagent.matrix.transport.router.hash.HashRouterUtils;
import com.tencent.angel.psagent.matrix.transport.router.range.RangeRouterUtils;

public class RouterUtils {

  //////////////////////////////////////////////////////////////////////////////////////////////////
  // Keys split
  //////////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Split keys by matrix partition
   *
   * @param matrixMeta matrix meta data
   * @param keys the keys need partitioned
   * @return partition key to key partition map
   */
  public static KeyPart[] split(MatrixMeta matrixMeta, int rowId, int[] keys) {
    return split(matrixMeta, rowId, keys, false);
  }

  /**
   * Split keys by matrix partition
   *
   * @param matrixMeta matrix meta data
   * @param keys the keys need partitioned
   * @return partition key to key partition map
   */
  public static KeyPart[] split(MatrixMeta matrixMeta, int rowId, long[] keys) {
    return split(matrixMeta, rowId, keys, false);
  }

  /**
   * Split keys by matrix partition
   *
   * @param matrixMeta matrix meta data
   * @param keys the keys need partitioned
   * @return partition key to key partition map
   */
  public static KeyPart[] split(MatrixMeta matrixMeta, int rowId, String[] keys) {
    return split(matrixMeta, rowId, keys, false);
  }

  /**
   * Split keys by matrix partition
   *
   * @param matrixMeta matrix meta data
   * @param keys the keys need partitioned
   * @return partition key to key partition map
   */
  public static KeyPart[] split(MatrixMeta matrixMeta, int rowId, IElement[] keys) {
    return split(matrixMeta, rowId, keys, false);
  }

  /**
   * Split keys by matrix partition
   *
   * @param matrixMeta matrix meta data
   * @param keys the keys need partitioned
   * @return partition key to key partition map
   */
  public static KeyPart[] split(MatrixMeta matrixMeta, int rowId, int[] keys, boolean isSorted) {
    if(matrixMeta.isHash()) {
      return HashRouterUtils.split(matrixMeta, rowId, keys);
    } else {
      return RangeRouterUtils.split(matrixMeta, rowId, keys, isSorted);
    }
  }

  /**
   * Split keys by matrix partition
   *
   * @param matrixMeta matrix meta data
   * @param keys the keys need partitioned
   * @return partition key to key partition map
   */
  public static KeyPart[] split(MatrixMeta matrixMeta, int rowId, long[] keys, boolean isSorted) {
    if(matrixMeta.isHash()) {
      return HashRouterUtils.split(matrixMeta, rowId, keys);
    } else {
      return RangeRouterUtils.split(matrixMeta, rowId, keys, isSorted);
    }
  }

  /**
   * Split keys by matrix partition
   *
   * @param matrixMeta matrix meta data
   * @param keys the keys need partitioned
   * @return partition key to key partition map
   */
  public static KeyPart[] split(MatrixMeta matrixMeta, int rowId, String[] keys, boolean isSorted) {
    if(matrixMeta.isHash()) {
      return HashRouterUtils.split(matrixMeta, rowId, keys);
    } else {
      return RangeRouterUtils.split(matrixMeta, rowId, keys, isSorted);
    }
  }

  /**
   * Split keys by matrix partition
   *
   * @param matrixMeta matrix meta data
   * @param keys the keys need partitioned
   * @return partition key to key partition map
   */
  public static KeyPart[] split(MatrixMeta matrixMeta, int rowId, IElement[] keys, boolean isSorted) {
    if(matrixMeta.isHash()) {
      return HashRouterUtils.split(matrixMeta, rowId, keys);
    } else {
      return RangeRouterUtils.split(matrixMeta, rowId, keys, isSorted);
    }
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////
  // Keys and values split: int/long/string/object key, float value
  //////////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Split keys by matrix partition
   *
   * @param matrixMeta matrix meta data
   * @param keys the keys need partitioned
   * @return partition key to key partition map
   */
  public static KeyValuePart[] split(MatrixMeta matrixMeta, int rowId, int[] keys,
      float[] values) {
    return split(matrixMeta, rowId, keys, values, false);
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
    return split(matrixMeta, rowId, keys, values, false);
  }


  /**
   * Split keys by matrix partition
   *
   * @param matrixMeta matrix meta data
   * @param keys the keys need partitioned
   * @return partition key to key partition map
   */
  public static KeyValuePart[] split(MatrixMeta matrixMeta, int rowId, int[] keys,
      float[] values, boolean isSorted) {
    if(matrixMeta.isHash()) {
      return HashRouterUtils.split(matrixMeta, rowId, keys, values);
    } else {
      return RangeRouterUtils.split(matrixMeta, rowId, keys, values, isSorted);
    }
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
      float[] values, boolean isSorted) {
    if(matrixMeta.isHash()) {
      return HashRouterUtils.split(matrixMeta, rowId, keys, values);
    } else {
      return RangeRouterUtils.split(matrixMeta, rowId, keys, values, isSorted);
    }
  }


  //////////////////////////////////////////////////////////////////////////////////////////////////
  // Keys and values split: int/long/string/object key, double value
  //////////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Split keys by matrix partition
   *
   * @param matrixMeta matrix meta data
   * @param keys the keys need partitioned
   * @return partition key to key partition map
   */
  public static KeyValuePart[] split(MatrixMeta matrixMeta, int rowId, int[] keys,
      double[] values) {
    return split(matrixMeta, rowId, keys, values, false);
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
    return split(matrixMeta, rowId, keys, values, false);
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
    if(matrixMeta.isHash()) {
      return HashRouterUtils.split(matrixMeta, rowId, keys, values);
    } else {
      return RangeRouterUtils.split(matrixMeta, rowId, keys, values, isSorted);
    }
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
    if(matrixMeta.isHash()) {
      return HashRouterUtils.split(matrixMeta, rowId, keys, values);
    } else {
      return RangeRouterUtils.split(matrixMeta, rowId, keys, values, isSorted);
    }
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////
  // Keys and values split: int/long/string/object key, long value
  //////////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Split keys by matrix partition
   *
   * @param matrixMeta matrix meta data
   * @param keys the keys need partitioned
   * @return partition key to key partition map
   */
  public static KeyValuePart[] split(MatrixMeta matrixMeta, int rowId, int[] keys,
      long[] values) {
    return split(matrixMeta, rowId, keys, values, false);
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
      long[] values) {
    return split(matrixMeta, rowId, keys, values, false);
  }


  /**
   * Split keys by matrix partition
   *
   * @param matrixMeta matrix meta data
   * @param keys the keys need partitioned
   * @return partition key to key partition map
   */
  public static KeyValuePart[] split(MatrixMeta matrixMeta, int rowId, int[] keys,
      long[] values, boolean isSorted) {
    if(matrixMeta.isHash()) {
      return HashRouterUtils.split(matrixMeta, rowId, keys, values);
    } else {
      return RangeRouterUtils.split(matrixMeta, rowId, keys, values, isSorted);
    }
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
    if(matrixMeta.isHash()) {
      return HashRouterUtils.split(matrixMeta, rowId, keys, values);
    } else {
      return RangeRouterUtils.split(matrixMeta, rowId, keys, values, isSorted);
    }
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////
  // Keys and values split: int/long/string/object key, object value
  //////////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Split keys by matrix partition
   *
   * @param matrixMeta matrix meta data
   * @param keys the keys need partitioned
   * @return partition key to key partition map
   */
  public static KeyValuePart[] split(MatrixMeta matrixMeta, int rowId, int[] keys,
      IElement[] values) {
    return split(matrixMeta, rowId, keys, values, false);
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
    return split(matrixMeta, rowId, keys, values, false);
  }


  /**
   * Split keys by matrix partition
   *
   * @param matrixMeta matrix meta data
   * @param keys the keys need partitioned
   * @return partition key to key partition map
   */
  public static KeyValuePart[] split(MatrixMeta matrixMeta, int rowId, int[] keys,
      IElement[] values, boolean isSorted) {
    if(matrixMeta.isHash()) {
      return HashRouterUtils.split(matrixMeta, rowId, keys, values);
    } else {
      return RangeRouterUtils.split(matrixMeta, rowId, keys, values, isSorted);
    }
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
    if(matrixMeta.isHash()) {
      return HashRouterUtils.split(matrixMeta, rowId, keys, values);
    } else {
      return RangeRouterUtils.split(matrixMeta, rowId, keys, values, isSorted);
    }
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
    return HashRouterUtils.split(matrixMeta, rowId, keys, values);
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
    return HashRouterUtils.split(matrixMeta, rowId, keys, values);
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////
  // Keys and values split: vector
  //////////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Split keys by matrix partition
   *
   * @param matrixMeta matrix meta data
   * @param vector Matrix vector
   * @return partition key to key partition map
   */
  public static KeyValuePart[] split(MatrixMeta matrixMeta, Vector vector) {
    if(matrixMeta.isHash()) {
      return HashRouterUtils.split(matrixMeta, vector);
    } else {
      return RangeRouterUtils.split(matrixMeta, vector);
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
    if(matrixMeta.isHash()) {
      return HashRouterUtils.splitStream(matrixMeta, vector);
    } else {
      return RangeRouterUtils.splitStream(matrixMeta, vector);
    }
  }

  /**
   * Split keys by matrix partition
   *
   * @param matrixMeta matrix meta data
   * @param matrix Matrix vector
   * @return partition key to key partition map
   */
  public static CompStreamKeyValuePart[] splitStream(MatrixMeta matrixMeta, Matrix matrix) {
    return null;
  }

  /**
   * Split keys by matrix partition
   *
   * @param matrixMeta matrix meta data
   * @param vectors Matrix vector
   * @return partition key to key partition map
   */
  public static CompStreamKeyValuePart[] splitStream(MatrixMeta matrixMeta, Vector[] vectors) {
    if(matrixMeta.isHash()) {
      return HashRouterUtils.splitStream(matrixMeta, vectors);
    } else {
      return RangeRouterUtils.splitStream(matrixMeta, vectors);
    }
  }
}

/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tencent.angel.ml.feature;

import com.tencent.angel.ml.math.vector.SparseDoubleSortedVector;
import com.tencent.angel.ml.math.vector.SparseDoubleVector;
import com.tencent.angel.ml.math.vector.SparseDummyVector;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * generating feature with raw input
 */
public class Feature {

  private static final Log LOG = LogFactory.getLog(Feature.class);

  public static LabeledData getDummmyFeatureFromLibsvm(String line, int dimension, int nonzero) {
    if (null == line) {
      return null;
    }

    String[] splits = line.split(" ");

    if (splits.length < 1) {
      return null;
    }

    SparseDummyVector x = new SparseDummyVector(dimension, nonzero);

    double y = Double.parseDouble(splits[0]);

    if (y != 1) {
      y = -1;
    }

    for (int i = 1; i < splits.length; i++) {
      String temp = splits[i];
      int seperator = temp.indexOf(":");
      if (seperator != -1) {
        int index = Integer.parseInt(temp.substring(0, seperator));
        double value = Double.parseDouble(temp.substring(seperator + 1));
        x.set(index - 1, value); // the index in libsvm file begin with 1
        // LOG.debug(String.format("index: %d, value: %s", index, value));
      }
    }

    LabeledData result = new LabeledData(x, y);

    return result;
  }

  public static LabeledData getFeatureFromLibsvm(String line, int dimension, int nonzero) {
    if (null == line) {
      return null;
    }

    String[] splits = line.split(" ");

    if (splits.length < 1) {
      return null;
    }

    SparseDoubleVector x = new SparseDoubleVector(dimension, nonzero);

    double y = Double.parseDouble(splits[0]);

    for (int i = 1; i < splits.length; i++) {
      String temp = splits[i];
      int seperator = temp.indexOf(":");
      if (seperator != -1) {
        int index = Integer.parseInt(temp.substring(0, seperator));
        double value = Double.parseDouble(temp.substring(seperator + 1));
        x.set(index - 1, value); // the index in libsvm file begin with 1
        // LOG.debug(String.format("index: %d, value: %s", index, value));
      }
    }

    LabeledData result = new LabeledData(x, y);

    return result;
  }

  public static LabeledData getFeatureFromLibsvm(String line, int dimension) {
    return getFeatureFromLibsvm(line, dimension, dimension);
  }

  public static LabeledData getSparseSortedFeatureFromLibsvm(String line, int dimension) {
    if (null == line) {
      return null;
    }
    String[] splits = line.trim().split(" ");
    if (splits.length < 1) {
      return null;
    }
    int nonzero = splits.length - 1;
    SparseDoubleSortedVector x = new SparseDoubleSortedVector(nonzero, dimension);
    double y = Double.parseDouble(splits[0]);
    if (y != 1) {
      y = -1;
    }
    for (int i = 1; i < splits.length; i++) {
      String temp = splits[i];
      int seperator = temp.indexOf(":");
      if (seperator != -1) {
        int index = Integer.parseInt(temp.substring(0, seperator));
        double value = Double.parseDouble(temp.substring(seperator + 1));
        x.set(index - 1, value); // the index in libsvm file begin with 1
        // LOG.debug(String.format("index: %d, value: %s", index, value));
      }
    }

    LabeledData result = new LabeledData(x, y);

    return result;
  }

  public static LabeledData getSparseSortedFeatureFromCSV(String line, int dimension, int nonzero) {
    if (null == line) {
      return null;
    }
    String[] splits = line.split(",");
    if (splits.length < 1) {
      return null;
    }
    SparseDoubleSortedVector x = new SparseDoubleSortedVector(nonzero, dimension);
    double y = Double.parseDouble(splits[splits.length - 1]);
    if (y != 1)
      y = 0;
    for (int i = 0; i < splits.length - 1; i++) {
      double value = Double.parseDouble(splits[i]);
      x.set(i, value);
    }

    LabeledData result = new LabeledData(x, y);
    return result;
  }

  public static LabeledData getFeatureFromRealData(String line, int dimension, int nonzero) {
    if (null == line) {
      return null;
    }

    String[] splits = line.split(",");

    if (splits.length < 1) {
      return null;
    }

    SparseDummyVector x = new SparseDummyVector(dimension, splits.length - 1);

    double y = Double.parseDouble(splits[0]);

    if (y != 1) {
      y = -1;
    }

    for (int i = 1; i < splits.length; i++) {
      int index = Integer.parseInt(splits[i]);
      x.set(index, 1);
      // LOG.debug(String.format("index: %d, value: %s", index, value));
    }

    LabeledData result = new LabeledData(x, y);

    return result;
  }

  public static LabeledData getFeatureFromRealDataForRegression(String line, int dimension,
      boolean addIntercept) {
    if (null == line) {
      return null;
    }

    String[] splits = line.split(",");

    if (splits.length < 1) {
      return null;
    }

    SparseDummyVector x = new SparseDummyVector(dimension, splits.length + 1);

    double y = Double.parseDouble(splits[0]);

    for (int i = 1; i < splits.length; i++) {
      int index = Integer.parseInt(splits[i]);
      x.set(index, 1);
    }

    if (addIntercept) {
      x.set(dimension, 1);
    }

    LabeledData result = new LabeledData(x, y);

    return result;
  }

  public static LabeledData getFeatureFromRealData(long[] data, int dimension, int nonzero) {
    if (null == data) {
      return null;
    }

    if (data.length < 1) {
      return null;
    }

    SparseDummyVector x = new SparseDummyVector(dimension, nonzero);

    double y = data[0];

    if (y != 1) {
      y = -1;
    }

    for (int i = 1; i < data.length; i++) {
      x.set((int) data[i], 1);
    }

    LabeledData result = new LabeledData(x, y);
    return result;
  }

  public static LabeledData getFeatureFromRealData(double target, long[] data, int dimension,
      int nonzero) {
    if (null == data) {
      return null;
    }

    if (data.length < 1) {
      return null;
    }

    SparseDummyVector x = new SparseDummyVector(dimension, nonzero);

    for (int i = 0; i < data.length; i++) {
      x.set((int) data[i], 1);
    }

    // LOG.debug("target=" + target);
    LabeledData result = new LabeledData(x, target);
    return result;
  }
}

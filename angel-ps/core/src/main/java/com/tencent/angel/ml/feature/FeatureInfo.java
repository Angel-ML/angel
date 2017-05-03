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

import com.tencent.angel.exception.InvalidParameterException;

import java.util.*;

public class FeatureInfo {

  public class Match {
    public Match(String fieldName, int index) {
      super();
      this.fieldName = fieldName;
      this.index = index;
    }

    public String fieldName;
    public int index;
  }

  public class SimpleFeature {
    public SimpleFeature(String featureName) {
      super();
      this.featureName = featureName;
    }

    public String featureName;
  }

  public class CombineFeature {
    public CombineFeature(String featureName, String[] dependFeatures) {
      super();
      this.featureName = featureName;
      this.dependFeatures = dependFeatures;
    }

    public String featureName;
    public String[] dependFeatures;

    public int dependFeatureSize() {
      return dependFeatures.length;
    }
  }

  public final Map<String, Integer> matchMap;
  public final List<SimpleFeature> simpleFeatureList;
  public final List<CombineFeature> combineFeatures;

  public String[] simpleFeatureNames;
  public int[] simpleFeatureIndexes;
  public String[][] combineFeatureNames;
  public int[][] combineFeatureIndexes;

  public FeatureInfo() {
    // this.matchList = new ArrayList<Match>();
    this.matchMap = new HashMap<String, Integer>();
    this.simpleFeatureList = new ArrayList<SimpleFeature>();
    this.combineFeatures = new ArrayList<CombineFeature>();
  }

  public void addMatch(String name, int index) {
    matchMap.put(name, index);
  }

  public int findMatchIndex(String name) {
    return matchMap.get(name);
  }

  public void addSimpleFeature(String name) {
    simpleFeatureList.add(new SimpleFeature(name));
  }

  public void addCombineFeatures(String name, String[] dependFeatures) {
    combineFeatures.add(new CombineFeature(name, dependFeatures));
  }

  public int getMatchSize() {
    return matchMap.size();
  }

  public Map<String, Integer> getMatchMap() {
    return matchMap;
  }

  public List<SimpleFeature> getSimpleFeatureList() {
    return simpleFeatureList;
  }

  public List<CombineFeature> getCombineFeatures() {
    return combineFeatures;
  }

  public String[][] getCombineFeatureNames() {
    if (combineFeatureNames == null) {
      combineFeatureNames = new String[combineFeatures.size()][];
      for (int i = 0; i < combineFeatures.size(); i++) {
        combineFeatureNames[i] = combineFeatures.get(i).dependFeatures;
      }
    }

    return combineFeatureNames;
  }

  public String[] getSimpleFeatureNames() {
    if (simpleFeatureNames == null) {
      simpleFeatureNames = new String[simpleFeatureList.size()];
      for (int i = 0; i < simpleFeatureNames.length; i++) {
        simpleFeatureNames[i] = simpleFeatureList.get(i).featureName;
      }
    }

    return simpleFeatureNames;
  }

  public int[] getSimpleFeatureIndexes() throws InvalidParameterException {
    if (simpleFeatureIndexes == null) {
      simpleFeatureIndexes = new int[simpleFeatureList.size()];

      for (int i = 0; i < simpleFeatureIndexes.length; i++) {
        Integer index = matchMap.get(simpleFeatureList.get(i).featureName);
        if (index == null) {
          throw new InvalidParameterException("feature " + simpleFeatureList.get(i).featureName
              + " can not found in match list");
        }
        simpleFeatureIndexes[i] = index;
      }
    }

    return simpleFeatureIndexes;
  }

  public int[][] getCombineFeatureIndexes() throws InvalidParameterException {
    if (combineFeatureIndexes == null) {
      combineFeatureIndexes = new int[combineFeatures.size()][];

      for (int i = 0; i < combineFeatureIndexes.length; i++) {
        combineFeatureIndexes[i] = new int[combineFeatures.get(i).dependFeatures.length];
        for (int j = 0; j < combineFeatureIndexes[i].length; j++) {
          Integer index = matchMap.get(combineFeatures.get(i).dependFeatures[j]);
          if (index == null) {
            throw new InvalidParameterException("feature "
                + combineFeatures.get(i).dependFeatures[j] + " can not found in match list");
          }
          combineFeatureIndexes[i][j] = index;
        }
      }
    }

    return combineFeatureIndexes;
  }

  @Override
  public String toString() {
    return "FeatureInfo [matchMap=" + matchMap + ", simpleFeatureList=" + simpleFeatureList
        + ", combineFeatures=" + combineFeatures + ", simpleFeatureNames="
        + Arrays.toString(simpleFeatureNames) + ", simpleFeatureIndexes="
        + Arrays.toString(simpleFeatureIndexes) + ", combineFeatureNames="
        + Arrays.toString(combineFeatureNames) + ", combineFeatureIndexes="
        + Arrays.toString(combineFeatureIndexes) + "]";
  }

}

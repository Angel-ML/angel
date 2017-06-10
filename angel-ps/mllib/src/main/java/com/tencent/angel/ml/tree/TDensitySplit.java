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
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */
package com.tencent.angel.ml.tree;

import com.tencent.angel.ml.RegTree.DataMeta;
import com.tencent.angel.ml.RegTree.RegTMaker;
import com.tencent.angel.ml.utils.MathUtils;
import com.tencent.angel.utils.Sort;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Description: get candidate split value, according to density of data distribution
 */

public class TDensitySplit implements TSplitValueHelper {

  private static final Log LOG = LogFactory.getLog(TDensitySplit.class);
  private int splitCandidNum;
  private int splitNum;

  public TDensitySplit(int splitCandidNum, int splitNum) {
    this.splitCandidNum = splitCandidNum;
    this.splitNum = splitNum;
  }

  /**
   * Get split value of each feature of a node in a tree. currently, we generate all the features
   * Todo: generate sampled features
   * 
   * @param dataMeta the data meta info
   * @param treeMaker the tree maker
   * @param nid the node id in the tree
   * @return the float[][] split value, one row for one feature
   */
  @Override
  public float[][] getSplitValue(DataMeta dataMeta, RegTMaker treeMaker, int nid) {
    int numFeature = dataMeta.featureMeta.numFeature;
    float[][] splitSet = new float[numFeature][splitNum];
    int[][] counts = new int[numFeature][splitCandidNum];

    double[] binWidths = new double[numFeature];

    for (int i = 0; i < numFeature; i++) {
      int fid = i;
      binWidths[i] =
          (dataMeta.featureMeta.maxFeatures[fid] - dataMeta.featureMeta.minFeatures[fid])
              / (splitCandidNum + 1);
    }

    // insIdx is the index of instances
    for (int insIdx = 0; insIdx < dataMeta.numRow; insIdx++) {
      if (treeMaker.ins2Node.get(insIdx) == nid) {
        int[] indices = dataMeta.instances.get(insIdx).getIndices();
        double[] values = dataMeta.instances.get(insIdx).getValues();
        for (int i = 0; i < indices.length; i++) {
          int findex = indices[i]; // feature index
          double binWidth = binWidths[findex]; // bin width
          double fvalue = (float) values[i]; // feature value
          int binIndex =
              (int) Math.floor((fvalue - dataMeta.featureMeta.minFeatures[findex]) / binWidth);
          counts[findex][binIndex]++;
        }
      }
    }

    // loop over features
    for (int fid = 0; fid < numFeature; fid++) {
      int[] minCandid = findMinCandid(counts[fid], splitNum);
      for (int i = 0; i < minCandid.length; i++) {
        splitSet[fid][i] = minCandid[i];
      }
    }

    return splitSet;
  }

  public static int[] findMinCandid(int[] counts, int splitNum) {

    int[] nzzIdxes = findNzzIndex(counts);


    if (nzzIdxes.length <= splitNum) {
      int[] minCandid = new int[nzzIdxes.length - 1];
      int[] nzzCounts = new int[nzzIdxes.length];
      for (int i = 0; i < nzzCounts.length; i++) {
        nzzCounts[i] = counts[nzzIdxes[i]];
      }
      Sort.quickSort(nzzCounts, nzzIdxes, 0, nzzCounts.length - 1);
      System.arraycopy(nzzIdxes, 0, minCandid, 0, nzzIdxes.length - 1);
      return minCandid;
    } else {
      int[] minCandid = new int[splitNum];
      int[] idxes = new int[counts.length];
      for (int i = 0; i < counts.length; i++) {
        idxes[i] = i + 1;
      }
      Sort.quickSort(counts, idxes, 0, counts.length - 1);
      System.arraycopy(idxes, 0, minCandid, 0, splitNum);
      return minCandid;
    }
  }

  public static int[] findNzzIndex(int[] counts) {
    List<Integer> nzzIdxes = new ArrayList<Integer>();
    for (int i = 0; i < counts.length; i++) {
      if (counts[i] > 0) {
        nzzIdxes.add(i);
      }
    }
    return MathUtils.intList2Arr(nzzIdxes);
  }

  public static void main(String[] argv) {
    int[] counts = {10, 11, 2, 21, 8, 3, 13, 5};
    // int[] counts = {10, 2, 1, 2, 0, 3, 0, 5};
    int splitNum = 3;
    int[] idxes = findMinCandid(counts, splitNum);
    for (int idx : idxes) {
      System.out.println(idx);
    }
  }
}

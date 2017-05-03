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
package com.tencent.angel.ml.algorithm.RegTree;

import com.tencent.angel.ml.algorithm.param.RegTTrainParam;
import com.tencent.angel.ml.math.vector.SparseDoubleSortedVector;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;

/**
 * Description: a set of histograms from different feature index
 */

public class HistSet {

  private static final Log LOG = LogFactory.getLog(HistSet.class);


  private RegTTrainParam param;
  private int nid; // node id
  public int[] fset; // feature id
  // map feature id to the position in units
  public Map<Integer, Integer> fea2Unit;
  public List<HistUnit> units; // hist units

  public HistSet(RegTree tree, RegTTrainParam param, int nid) {
    this.param = param;
    this.nid = nid;
    this.fset = tree.fset;
    fea2Unit = new HashMap<Integer, Integer>();
    this.units = new ArrayList<HistUnit>();
  }

  public void build(RegTMaker treeMaker, DataMeta dataMeta, float[][] candidSplitValue) {
    // add hist unit of each feature
    for (int i = 0; i < fset.length; i++) {
      LOG.info(String.format("Candidate split value of fid[%d]: %s", fset[i],
          Arrays.toString(candidSplitValue[i])));
      units.add(new HistUnit(nid, fset[i], param, candidSplitValue[i]));
      fea2Unit.put(fset[i], i);
    }
    // loop instance's position, find those belong to nid
    for (int insIdx = 0; insIdx < treeMaker.ins2Node.size(); insIdx++) {
      if (treeMaker.ins2Node.get(insIdx) == nid) {
        SparseDoubleSortedVector instance = dataMeta.instances.get(insIdx);
        int[] indices = instance.getIndices();
        double[] values = instance.getValues();
        float label = dataMeta.labels[insIdx];
        for (int i = 0; i < indices.length; i++) {
          int fid = indices[i];
          float fv = (float) values[i];
          units.get(fea2Unit.get(fid)).add(fid, fv, treeMaker.gradPairs.get(insIdx));
        }
      }
    }
    printHist();
  }

  public void setFset(int[] fset) {
    this.fset = fset;
  }

  public void setUnits(List<HistUnit> units) {
    this.units = units;
  }

  public int getFid(int i) {
    return fset[i];
  }

  public HistUnit getUnit(int i) {
    return units.get(i);
  }

  public void printHist() {
    for (int i = 0; i < fset.length; i++) {
      int fid = fset[i];
      String unitString = units.get(fea2Unit.get(fid)).toString();
      LOG.info("Feature id: " + fid + ", HistUnit: " + unitString);
    }
  }
}

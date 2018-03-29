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

package com.tencent.angel.ml.treemodels.tree.basic;

import com.tencent.angel.ml.treemodels.param.TreeParam;

import java.util.HashMap;
import java.util.Map;

public abstract class Tree<TParam extends TreeParam, Node extends TNode> {
  protected final TParam param;
  private int[] fset; // features used in this tree, null means all the features are used
  protected Map<Integer, Node> nodes; // nodes in the tree

  public Tree(TParam param) {
    this.param = param;
    this.nodes = new HashMap<>();
  }

  public int[] getFset() {
    return this.fset;
  }

  public Node getRoot() {
    return this.nodes.get(0);
  }

  public Node getNode(int nid) {
    return this.nodes.get(nid);
  }

  public void setNode(int nid, Node node) {
    this.nodes.put(nid, node);
  }

  public void setFset(int[] fset) {
    this.fset = fset;
  }
}

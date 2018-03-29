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

package com.tencent.angel.ml.treemodels.tree.regression;

import com.tencent.angel.ml.treemodels.param.RegTParam;
import com.tencent.angel.ml.treemodels.tree.basic.Tree;

public class RegTree extends Tree<RegTParam, RegTNode> {

  public RegTree(RegTParam param) {
    super(param);
    RegTNode root = new RegTNode(0, null, param.numClass);
    this.nodes.put(0, root);
  }
}

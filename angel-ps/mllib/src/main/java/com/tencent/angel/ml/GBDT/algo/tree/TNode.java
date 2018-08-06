/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in 
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */
package com.tencent.angel.ml.GBDT.algo.tree;

/**
 * Description: Basic Tree node
 */

public class TNode {

  public int nid; // node id in the tree
  public int parent; // node id of parent in the tree, start with 0
  public int leftChild; // node id of left child in the tree
  public int rightChild; // node id of right child in the tree
  private boolean isLeaf;
  private float leafValue;

  public TNode(int nid, int parent, int left, int right) {
    this.parent = parent;
    this.leftChild = left;
    this.rightChild = right;
    this.isLeaf = false;
  }

  public TNode(int nid) {
    new TNode(nid, -1, -1, -1);
  }

  public TNode() {
    new TNode(-1);
  }

  public boolean isLeaf() {
    return isLeaf;
  }

  public void chgToLeaf() {
    this.isLeaf = true;
  }

  public int getParent() {
    return parent;
  }

  public void setParent(int parent) {
    this.parent = parent;
  }

  public int getLeftChild() {
    return leftChild;
  }

  public void setLeftChild(int leftChild) {
    this.leftChild = leftChild;
  }

  public int getRightChild() {
    return rightChild;
  }

  public void setRightChild(int rightChild) {
    this.rightChild = rightChild;
  }

  public float getLeafValue() {
    return leafValue;
  }

  public void setLeafValue(float leafValue) {
    this.leafValue = leafValue;
  }

  public int getNid() {
    return nid;
  }

  public void setNid(int nid) {
    this.nid = nid;
  }
}

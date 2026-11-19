package com.tencent.angel.graph.data;

import com.tencent.angel.ps.storage.vector.element.IElement;

public abstract class Neighbor implements IElement {

  public abstract NeighborStorageType getStorageType();

  public abstract int getNeighborsNum();

  public abstract Neighbor sampleNeighbors(int count, Boolean shuffle, long seed);

  public abstract Neighbor sampleNeighbors(int count, int startPos);

}

package com.tencent.angel.graph.client.psf.sample.sampleneighbor;

import java.util.HashMap;
import java.util.Map;


/**
 * Sample type,such as : SIMPLE, NODE, EDGE, NODE_AND_EDGE; SIMPLE: sample neighbors randomly by
 * number count. NODE: sample neighbors randomly with neighbor type EDGE: sample neighbors randomly
 * with edge type by number count NODE_AND_EDGE: sample neighbors randomly with neighbor type and
 * edge type by number count
 */
public enum SampleType {
  SIMPLE(0),
  NODE(1),
  EDGE(2),
  NODE_AND_EDGE(3),
  EDGE_TIMESTAMP(4);

  static final Map<Integer, SampleType> indexToOpMap = new HashMap<>();

  static {
    indexToOpMap.put(SIMPLE.index, SIMPLE);
    indexToOpMap.put(NODE.index, NODE);
    indexToOpMap.put(EDGE.index, EDGE);
    indexToOpMap.put(NODE_AND_EDGE.index, NODE_AND_EDGE);
    indexToOpMap.put(EDGE_TIMESTAMP.index, EDGE_TIMESTAMP);
  }

  private final int index;

  SampleType(int index) {
    this.index = index;
  }

  public static SampleType valueOf(int index) {
    return indexToOpMap.get(index);
  }

  public int getIndex() {
    return index;
  }
}

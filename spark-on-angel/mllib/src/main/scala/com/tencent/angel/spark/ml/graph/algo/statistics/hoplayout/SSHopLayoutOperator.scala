package com.tencent.angel.spark.ml.graph.algo.statistics.hoplayout

import com.tencent.angel.spark.ml.graph.utils.GraphIO
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

object SSHopLayoutOperator extends Serializable{
  def selfCCEdgesDetection(ccpath: String, edges: RDD[(Long, Iterable[Long])], srcNode: Long, partNum: Int): RDD[(Long, Iterable[Long])] = {
    val ccInfo = loadCCInfo(ccpath)
    ccInfo.persist(StorageLevel.MEMORY_ONLY_SER)
    val tag = ccInfo.filter(_._1 == srcNode).collect()
    assert(tag.length == 1, "error in cc info file")
    
    val lbl = tag(0)._2
    val subNodes = ccInfo.filter(_._2 == lbl)
    
    edges.join(subNodes, partNum).map(ed => (ed._1, ed._2._1))
  }
  
  def loadCCInfo(ccpath: String): RDD[(Long, Long)] = {
    val df = GraphIO.load(ccpath, isWeighted = false, sep = "\t")
    df.select("src", "dst").rdd
      .filter(row => !row.anyNull)
      .map(row => (row.getLong(0), row.getLong(1)))
  }
}

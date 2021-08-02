package com.tencent.angel.graph.data

object CheckMotif {

  def srcNbrsMotifV2(src: Long, dst: Long, node: Long, src2dst: (Byte, Float), src2node: Byte): (Byte, Byte, Byte) = {
    var (srcIn, srcOut) = (0, 0)
    var (dstIn, dstOut) = (0, 0)
    var (nodeIn, nodeOut) = (0, 0)

    src2dst._1 match {
      case 0 => srcOut += 1; dstIn += 1
      case 1 => srcIn += 1; dstOut += 1
      case 2 => srcOut += 1; dstIn += 1; srcIn += 1; dstOut += 1
    }
    src2node match {
      case 0 => if (src < node) {
        srcOut += 1
        nodeIn += 1
      } else {
        nodeOut += 1
        srcIn += 1
      }
      case 1 => if (src < node) {
        srcIn += 1
        nodeOut += 1
      } else {
        srcOut += 1
        nodeIn += 1
      }
      case 2 => srcOut += 1; nodeIn += 1; srcIn += 1; nodeOut += 1
    }
    val degSeq = if (src < node) {
      srcOut + "" + srcIn + "" + dstOut + "" + dstIn + "" + nodeOut + "" + nodeIn
    } else {
      nodeOut + "" + nodeIn + "" + srcOut + "" + srcIn + "" + dstOut + "" + dstIn
    }
    val motifTag = motifTwoEdge.get(degSeq).get

    (motifTag(0), motifTag(1), motifTag(2))
  }

  def dstNbrsMotifV2(src: Long, dst: Long, node: Long, src2dst: (Byte, Float), dst2node: Byte): (Byte, Byte, Byte) = {
    var (srcIn, srcOut) = (0, 0)
    var (dstIn, dstOut) = (0, 0)
    var (nodeIn, nodeOut) = (0, 0)
    src2dst._1 match {
      case 0 => srcOut += 1; dstIn += 1
      case 1 => srcIn += 1; dstOut += 1
      case 2 => srcOut += 1; dstIn += 1; srcIn += 1; dstOut += 1
    }
    dst2node match {
      case 0 => if (dst < node) {
        dstOut += 1
        nodeIn += 1
      } else {
        dstIn += 1
        nodeOut += 1
      }
      case 1 => if (dst < node) {
        dstIn += 1
        nodeOut += 1
      } else {
        dstOut += 1
        nodeIn += 1
      }
      case 2 => dstOut += 1; nodeIn += 1; dstIn += 1; nodeOut += 1
    }
    val degSeq = if (node > src) {
      srcOut + "" + srcIn + "" + dstOut + "" + dstIn + "" + nodeOut + "" + nodeIn
    } else {
      nodeOut + "" + nodeIn + "" + dstOut + "" + dstIn + "" + srcOut + "" + srcIn
    }
    val motifTag = motifTwoEdge.get(degSeq).get
    (motifTag(0), motifTag(1), motifTag(2))
  }

  def motifTwoEdge = {
    val matrix = collection.mutable.Map[String, Array[Byte]]()

    matrix.put("011011", Array(3, 5, 4))
    matrix.put("011110", Array(3, 4, 5))
    matrix.put("110110", Array(4, 3, 5))
    matrix.put("111001", Array(4, 5, 3))
    matrix.put("101101", Array(5, 4, 3))
    matrix.put("100111", Array(5, 3, 4))

    matrix.put("010120", Array(7, 7, 6))
    matrix.put("012001", Array(7, 6, 7))
    matrix.put("200101", Array(6, 7, 7))

    matrix.put("101002", Array(9, 9, 8))
    matrix.put("100210", Array(9, 8, 9))
    matrix.put("021010", Array(8, 9, 9))

    matrix.put("111210", Array(15, 16, 17))
    matrix.put("111012", Array(15, 17, 16))
    matrix.put("101112", Array(17, 15, 16))
    matrix.put("101211", Array(17, 16, 15))
    matrix.put("121011", Array(16, 17, 15))
    matrix.put("121110", Array(16, 15, 17))

    matrix.put("110121", Array(18, 20, 19))
    matrix.put("112101", Array(18, 19, 20))
    matrix.put("211101", Array(19, 18, 20))
    matrix.put("210111", Array(19, 20, 18))
    matrix.put("012111", Array(20, 19, 18))
    matrix.put("011121", Array(20, 18, 19))
    matrix.put("221111", Array(21, 22, 22))
    matrix.put("112211", Array(22, 21, 22))
    matrix.put("111122", Array(22, 22, 21))
    matrix.toMap
  }

}

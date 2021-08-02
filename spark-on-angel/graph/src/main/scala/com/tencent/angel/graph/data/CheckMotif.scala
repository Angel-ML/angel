package com.tencent.angel.graph.data

import scala.collection.mutable.ArrayBuffer

object CheckMotif {

  /**
    * check the motif of a given triangle
    *
    * @param src
    * @param dst
    * @param node
    * @param src2dst
    * @param src2node
    * @param dst2node
    * @return
    */
  def triangleMotif(src: Long, dst: Long, node: Long, src2dst: Byte, src2node: Byte, dst2node: Byte): (Byte, Byte, Byte) = {
    var (srcIn, srcOut) = (0, 0)
    var (dstIn, dstOut) = (0, 0)
    var (nodeIn, nodeOut) = (0, 0)
    src2dst match {
      case 0 => srcOut += 1; dstIn += 1
      case 1 => srcIn += 1; dstOut += 1
      case 2 => srcOut += 1; dstIn += 1; srcIn += 1; dstOut += 1
    }

    src2node match {
      case 0 => srcOut += 1; nodeIn += 1
      case 1 => srcIn += 1; nodeOut += 1
      case 2 => srcOut += 1; nodeIn += 1; srcIn += 1; nodeOut += 1
    }

    dst2node match {
      case 0 => dstOut += 1; nodeIn += 1
      case 1 => dstIn += 1; nodeOut += 1
      case 2 => dstOut += 1; nodeIn += 1; dstIn += 1; nodeOut += 1
    }

    val degSeq = srcOut + "" + srcIn + "" + dstOut + "" + dstIn + "" + nodeOut + "" + nodeIn
    val motifTag = motifTriangle.get(degSeq).get
    (motifTag(0), motifTag(1), motifTag(2))
  }

  def srcNbrsMotif(src: Long, dst: Long, tag: Byte, srcNbrs: Array[(Long, Byte)],
                   dstNbrs: Array[(Long, Byte)], commonNbrs: Map[Long, (Byte, Byte)]): Array[((Long, Byte), Int)] = {
    val motifCount = new ArrayBuffer[((Long, Byte), Int)]() // (node,motifType)
    if (srcNbrs.length >= 1) {
      val filterMinView = srcNbrs.filter { case nbr => !commonNbrs.contains(nbr._1) && src < math.min(dst, nbr._1) && dst < nbr._1 }
      val filterMaxView = srcNbrs.filter { case nbr => !commonNbrs.contains(nbr._1) && dst > math.max(src, nbr._1) }
      var i = 0
      while (i <= 2) {
        val nbrs = filterMinView.filter(_._2 == i)
        if (nbrs.length > 0) {
          val (srcMotif, dstMotif, nodeMotif) = CheckMotif.srcNbrsMotif(src, dst, nbrs(0)._1, tag, i.toByte)
          motifCount.append(((src, srcMotif), nbrs.length))
          motifCount.append(((dst, dstMotif), nbrs.length))
        }

        val nbrsMaxSmall = filterMaxView.filter { case nbr => nbr._2 == i && nbr._1 < src }
        val nbrsMaxBig = filterMaxView.filter { case nbr => nbr._2 == i && nbr._1 > src }
        if (nbrsMaxSmall.length > 0) {
          val (srcMotif, dstMotif, nodeMotif) = CheckMotif.srcNbrsMotif(src, dst, nbrsMaxSmall(0)._1, tag, i.toByte)
          motifCount.append(((dst, nodeMotif), nbrsMaxSmall.length))
        }

        if (nbrsMaxBig.length > 0) {
          val (srcMotif, dstMotif, nodeMotif) = CheckMotif.srcNbrsMotif(src, dst, nbrsMaxBig(0)._1, tag, i.toByte)
          motifCount.append(((dst, dstMotif), nbrsMaxBig.length))
        }
        i += 1
      }
    }
    motifCount.toArray
  }

  def dstNbrsMotif(src: Long, dst: Long, tag: Byte, srcNbrs: Array[(Long, Byte)],
                   dstNbrs: Array[(Long, Byte)], commonNbrs: Map[Long, (Byte, Byte)]): Array[((Long, Byte), Int)] = {
    val motifCount = new ArrayBuffer[((Long, Byte), Int)]() // (node,motifType)
    if (dstNbrs.length >= 1) {
      val filterMinView = dstNbrs.filter { case nbr => !commonNbrs.contains(nbr._1) && src < dst && src < math.min(dst, nbr._1) }
      val filterMaxView = dstNbrs.filter { case nbr => !commonNbrs.contains(nbr._1) && src > nbr._1 && dst > math.max(src, nbr._1) }
      var i = 0
      while (i <= 2) { // the tag of the edge (dst,dstNbr)
        val nbrSmall = filterMinView.filter { case nbr => nbr._2 == i && nbr._1 < dst }
        val nbrBig = filterMinView.filter { case nbr => nbr._2 == i && nbr._1 > dst }

        val nbrMax = filterMaxView.filter { case nbr => nbr._2 == i }

        if (nbrSmall.length > 0) {
          val (srcMotif, dstMotif, nodeMotif) = CheckMotif.dstNbrsMotif(src, dst, nbrSmall(0)._1, tag, i.toByte)
          motifCount.append(((src, srcMotif), nbrSmall.length))
          motifCount.append(((dst, dstMotif), nbrSmall.length))
        }

        if (nbrBig.length > 0) {
          val (srcMotif, dstMotif, nodeMotif) = CheckMotif.dstNbrsMotif(src, dst, nbrBig(0)._1, tag, i.toByte)
          motifCount.append(((src, srcMotif), nbrBig.length))
          motifCount.append(((dst, dstMotif), nbrBig.length))
        }

        if (nbrMax.length > 0) {
          val (srcMotif, dstMotif, nodeMotif) = CheckMotif.dstNbrsMotif(src, dst, nbrMax(0)._1, tag, i.toByte)
          motifCount.append(((src, nodeMotif), nbrMax.length))
        }
        i += 1
      }


    }

    motifCount.toArray
  }

  /**
    * compute non-closure motif of two edge =>((src,dst),and (src,srcNbr))
    *
    * @param src
    * @param dst
    * @param node
    * @param src2dst
    * @param src2node
    * @return
    */
  def srcNbrsMotif(src: Long, dst: Long, node: Long, src2dst: Byte, src2node: Byte): (Byte, Byte, Byte) = {
    var (srcIn, srcOut) = (0, 0)
    var (dstIn, dstOut) = (0, 0)
    var (nodeIn, nodeOut) = (0, 0)

    src2dst match {
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


  def srcNbrsMotifMaxView(src: Long, dst: Long, node: Long, src2dst: Byte, src2node: Byte): (Byte, Byte, Byte) = {
    var (srcIn, srcOut) = (0, 0)
    var (dstIn, dstOut) = (0, 0)
    var (nodeIn, nodeOut) = (0, 0)

    src2dst match {
      case 0 => srcOut += 1; dstIn += 1
      case 1 => srcIn += 1; dstOut += 1
      case 2 => srcOut += 1; dstIn += 1; srcIn += 1; dstOut += 1
    }
    src2node match {
      case 0 => srcOut += 1; nodeIn += 1
      case 1 => srcIn += 1; nodeOut += 1
      case 2 => srcOut += 1; nodeIn += 1; srcIn += 1; nodeOut += 1
    }
    val degSeq = srcOut + "" + srcIn + "" + dstOut + "" + dstIn + "" + nodeOut + "" + nodeIn
    val motifTag = motifTwoEdge.get(degSeq).get


    (motifTag(0), motifTag(1), motifTag(2))
  }

  /**
    * compute non-closure motif of two edge =>((src,dst),and (dst,dstNbr))
    *
    * @param src
    * @param dst
    * @param node
    * @param src2dst
    * @param dst2node
    * @return
    */
  def dstNbrsMotif(src: Long, dst: Long, node: Long, src2dst: Byte, dst2node: Byte): (Byte, Byte, Byte) = {
    var (srcIn, srcOut) = (0, 0)
    var (dstIn, dstOut) = (0, 0)
    var (nodeIn, nodeOut) = (0, 0)
    src2dst match {
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

  def motifTriangle = {
    val matrix = collection.mutable.Map[String, Array[Byte]]()
    matrix.put("200211", Array(11, 12, 13))
    matrix.put("201102", Array(11, 13, 12))
    matrix.put("022011", Array(12, 11, 13))
    matrix.put("021120", Array(12, 13, 11))
    matrix.put("112002", Array(13, 11, 12))
    matrix.put("110220", Array(13, 12, 11))

    matrix.put("111111", Array(10, 10, 10))

    matrix.put("222222", Array(33, 33, 33))

    matrix.put("022121", Array(23, 24, 24))
    matrix.put("210221", Array(24, 23, 24))
    matrix.put("212102", Array(24, 24, 23))

    matrix.put("201212", Array(25, 26, 26))
    matrix.put("122012", Array(26, 25, 26))
    matrix.put("121220", Array(26, 26, 25))

    matrix.put("121121", Array(27, 28, 29))
    matrix.put("122111", Array(27, 29, 28))
    matrix.put("111221", Array(28, 27, 29))
    matrix.put("112112", Array(28, 29, 27))
    matrix.put("211112", Array(29, 28, 27))
    matrix.put("211211", Array(29, 27, 28))

    matrix.put("122221", Array(30, 31, 32))
    matrix.put("122122", Array(30, 32, 31))
    matrix.put("221221", Array(31, 30, 32))
    matrix.put("222112", Array(31, 32, 30))
    matrix.put("212212", Array(32, 31, 30))
    matrix.put("211222", Array(32, 30, 31))

    matrix.put("222222", Array(33, 33, 33))
    matrix.toMap
  }
}

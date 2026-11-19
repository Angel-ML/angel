package com.tencent.angel.spark.examples.ml

import com.tencent.angel.spark.ml.util.{DFStruct, DataLoader, DataSaver}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}
import org.apache.spark.mllib.linalg.{Vectors => mllibVectors}
import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext}
import com.tencent.angel.spark.ml.core.{ArgsUtil => coreArgsUtil}

/**
 * Created by isakjiang on 2020-11-16
 */
object SVDCluster {

  //奇异值个数
  val K = "k"
  //是否计算左奇异向量
  val COMPUTEU = "computeU"
  //条件数倒数
  val RCOND = "rCond"
  //奇异值向量S的保存路径
  val OUTPUTS = "outputS"
  //特征值对应的特征向量的保存路径
  val OUTPUTV = "outputV"
  //左奇异向量U的保存路径,若为null则不计算矩阵U,否则计算矩阵U并保存
  val OUTPUTU = "outputU"

  def main(args: Array[String]) = {

    val params = coreArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn")
    val input = params.getOrElse("input", null)
    val outputS = params.getOrElse("outputS", null)
    val outputV = params.getOrElse("outputV", null)
    val outputU = params.getOrElse("outputU", null)
    val partitionNum = params.getOrElse("partitionNum", "10").toInt
    val sampleRate = params.getOrElse("sampleRate", "1.0").toDouble

    //feature cols for matrix
    val featureCols = params.getOrElse("featureCols", null)
    //object col for index of matrix
    val labelCol = params.getOrElse("labelCol", "0").toInt

    val sep = params.getOrElse("sep", "space") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
    }

    //算法参数
    val k = params.getOrElse("K", "3").toInt
    val rCond = params.getOrElse(RCOND, "1e-9").toDouble

    val sc = start(mode)

    run(sc,
      input,
      outputS,
      outputV,
      outputU,
      featureCols,
      labelCol,
      sampleRate,
      partitionNum,
      k,
      rCond,
      sep
    )
    stop()
  }

  def run(sc: SparkContext,
          input: String,
          outputS: String,
          outputV: String,
          outputU: String,
          featureCols: String,
          labelCol: Int,
          sampleRate: Double,
          partitionNum: Int,
          k: Int,
          rCond: Double,
          sep: String
         ) = {


    val svdTable = DataLoader.loadlabeled(sc, input, partitionNum, sampleRate, featureCols, labelCol, false, sep)


    val rows = svdTable.select(DFStruct.LABEL, DFStruct.FEATURE).rdd
      .map { case Row(label: Double, feature: Vector) =>
        IndexedRow(label.toLong, mllibVectors.fromML(feature))
      }
    rows.name = "svdRows"
    rows.cache()

    val mat: IndexedRowMatrix = new IndexedRowMatrix(rows)
    // Compute SVD.
    val computeU = if (outputU == null) false else true
    val svd = mat.computeSVD(k, computeU, rCond)

    //保存S
    val sToString = svd.s.toArray.map(_.toString)
    val svdS = sc.makeRDD(Array(sToString))
    DataSaver.save(svdS, outputS, sep)

    //保存V
    val singVec = svd.V
    val singVecR = singVec.numRows
    val singVecC = singVec.numCols
    val singVecArray = (0 until singVecR).map { case i =>
      val rowArray = (0 until singVecC).map { case j => singVec(i, j).toString }.toArray
      val iArray = Array(i.toString)
      iArray ++ rowArray
    }
    val svdV = sc.makeRDD(singVecArray)
    DataSaver.save(svdV, outputV, sep)

    //保存U
    if (computeU) {
      val matU = svd.U.rows
      //index与行向量组成数组
      val svdU = matU.map { x =>
        val vecToArray = x.vector.toArray.map(_.toString)
        val indexToArray = Array(x.index.toString)
        indexToArray ++ vecToArray
      }
      DataSaver.save(svdU, outputU, sep)
    }
    rows.unpersist()

  }

  def start(mode: String = "local"): SparkContext = {
    val conf = new SparkConf()
    conf.setMaster(mode)
    conf.setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    sc
  }

  def stop(): Unit = {
    SparkContext.getOrCreate().stop()
  }
}

package com.tencent.angel.spark.examples.cluster

import breeze.linalg.{DenseMatrix => BDM}
import com.tencent.angel.spark.ml.util.{DataLoader, DataSaver, LogUtils}
import org.apache.spark.mllib.linalg.{Matrices, Matrix}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext}
import com.tencent.angel.spark.ml.core.{ArgsUtil => coreArgsUtil}
import scala.collection.mutable.ArrayBuffer

/**
 * Created by isakjiang on 2020-11-16
 */
object MutualInformationExample {

  def main(args: Array[String]): Unit = {

    val params = coreArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn-cluster")
    val input = params.getOrElse("input", null)
    val partitionNum = params.getOrElse("partitionNum", "10").toInt
    val sampleRate = params.getOrElse("sampleRate", "1.0").toDouble

    val sep = params.getOrElse("sep", "space") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
    }

    val newColStr = params.getOrElse("newColStr", null)
    val oriColStr = params.getOrElse("oriColStr", null)
    val output = params.getOrElse("output", null)

    val sc = start(mode)
    run(sc, input, output, partitionNum, sampleRate, newColStr, oriColStr, sep)
    stop()
  }

  def run(sc: SparkContext,
          input: String,
          output: String,
          partitionNum: Int,
          sampleRate: Double,
          newColStr: String,
          oriColStr: String,
          sep: String) = {

    //new cols
    val parseCols = DataLoader.parseFeatureCols(newColStr)
    val newCols = if (parseCols != null) parseCols.flatMap { case (begin, end) => begin to end } else null

    //if new cols exist,compute the corr with each other
    if (newCols != null) {
      val newColsLeng = newCols.length
      //load the new cols
      val eachArray = DataLoader.loadTable(sc, input, partitionNum, sampleRate, newColStr, sep).rdd
        .map { case row: Row => row.toSeq.toArray.map(x => x.toString) }
        .cache()

      val sampleNum = eachArray.count().toDouble

      //entropy for each feature,that is H(x),and collect it at driver
      val newEntropy = comEntropy(eachArray, sampleNum)

      val eachMutArray = ArrayBuffer[Array[String]]()
      val betMutArray = ArrayBuffer[Array[String]]()
      //the last result to save on hdfs
      val resultMutArray = ArrayBuffer[Array[String]]()

      //if the new col is not one column,compute join entropy
      if (newColsLeng > 1) {
        val joinMulInfor = eachArray.map { x =>
          val arraySize = x.size
          val arrayIdVal = new ArrayBuffer[(Int, Int, String, String)]
          (0 until arraySize).map { i =>
            ((i + 1) until arraySize).map { j =>
              arrayIdVal += Tuple4(i, j, x(i), x(j))
            }
          }
          arrayIdVal
        }

        val eachMutInfo = mutInfor(joinMulInfor, newEntropy, newEntropy, sampleNum)

        eachArray.unpersist()

        //switch result:mulInfo and entropy to matrix
        val resultMatrix = array2Matrix(newColsLeng, eachMutInfo, newEntropy)
        //matrix to array[String]
        val mat2Array = (0 until newColsLeng).map { case i =>
          val rowArray = (0 until newColsLeng).map { case j => resultMatrix(i, j).toString }.toArray
          Array(newCols(i).toString) ++ rowArray
        }
        //        val result = sc.makeRDD(mat2Array, 2)
        //        DataSaver.save(result, outputEach)
        val firstEachMut = Array("X") ++ newCols.map(_.toString)
        eachMutArray += firstEachMut
        eachMutArray ++= mat2Array

      } else
        eachArray.unpersist()

      //between entropy
      val parseOriCols = DataLoader.parseFeatureCols(oriColStr)
      val oriCols = if (parseOriCols != null) parseOriCols.flatMap { case (begin, end) => begin to end } else null

      //if original cols also exist,compute the corr between new cols and original cols
      if (oriCols != null) {
        val oriColsLeng = oriCols.length
        val allCols = newCols ++ oriCols
        val allColsLeng = allCols.length

        val input4All = DataLoader.loadTable(sc, input, partitionNum, sampleRate, newColStr + "," + oriColStr, sep).rdd
          .map { row: Row => row.toSeq.toArray.map(x => x.toString) }.cache()

        //oriCols rdd
        val oriArray = input4All.map { x =>
          (newColsLeng until allColsLeng).map(i => x(i)).toArray
        }

        val oriEntropy = comEntropy(oriArray, sampleNum)

        //all new cols to original table
        val all2Ori = input4All.map { rowArray =>
          val arrayMut = new ArrayBuffer[(Int, Int, String, String)]

          (0 until newColsLeng).map { i =>
            (newColsLeng until allCols.length).map { j =>
              //i:the position in new cols,(j - newColsLeng):the position in original cols,
              //j:the position of original cols in input4All
              arrayMut += Tuple4(i, (j - newColsLeng), rowArray(i), rowArray(j))
            }
          }
          arrayMut
        }

        val betMutInfo = mutInfor(all2Ori, newEntropy, oriEntropy, sampleNum).sortBy(x => (x._1, x._2))
        input4All.unpersist()

        //regulate the result
        val betIdArray = (0 until newColsLeng).map { i =>
          val one2ArrayMut = (0 until oriColsLeng).map(x => betMutInfo(i * oriColsLeng + x)._3.toString).toArray
          Array(newCols(i).toString) ++ one2ArrayMut
        }

        //        val betMutResult = sc.makeRDD(betIdArray, 2)
        //        DataSaver.save(betMutResult, outputBet)
        val firstBetCorr = Array("X") ++ oriCols.map(_.toString)
        betMutArray += firstBetCorr
        betMutArray ++= betIdArray

      }

      //concat eachArray and betArray,which will be saved on resultArray
      if ((eachMutArray.length != 0) && (betMutArray.length != 0)) {
        //delete the first column in betArray
        val betDelFirst = betMutArray.map { iArray =>
          (1 until iArray.length).map(i => iArray(i))
        }

        (0 until eachMutArray.length).map { i =>
          resultMutArray += (eachMutArray(i) ++ betDelFirst(i))
        }
      }
      else if (eachMutArray.length != 0)
        resultMutArray ++= eachMutArray
      else if (betMutArray.length != 0)
        resultMutArray ++= betMutArray

      //save the result if resltArray is not null
      if (resultMutArray.length != 0) {
        val resultRdd = sc.makeRDD(resultMutArray, 1)
        DataSaver.save(resultRdd, output, sep)
      }
    }
  }

  //compute entropy,note:the result for id decided by the position of the input array
  def comEntropy(arrayStr: RDD[Array[String]], sampleCount: Double): Map[Int, Double] = {
    val idValue = arrayStr.map { x =>
      (0 until x.size).map(i => ((i, x(i)), 1l)).toArray
    }

    val idValFlat = idValue
      .flatMap(x => x)
      .reduceByKey(_ + _)
      .map { case ((id, value), valNum) => (id, valNum.toDouble) }

    //entropy for each feature,that is H(x),and collect it at driver
    idValFlat
      .map(x => (x._1, -(x._2 / sampleCount) * Math.log(x._2 / sampleCount)))
      .reduceByKey(_ + _)
      .collect()
      .toMap
  }

  def checkNumColumns(cols: Int): Unit = {
    if (cols > 65535) {
      throw new IllegalArgumentException(s"Argument with more than 65535 cols: $cols")
    }
    if (cols > 10000) {
      val memMB = (cols.toLong * cols) / 125000
      LogUtils.logTime(s"$cols columns will require at least $memMB megabytes of memory!")
    }
  }

  def array2Matrix(n: Int, U: Array[(Int, Int, Double)], idE: Map[Int, Double]): Matrix = {
    val G = new BDM[Double](n, n)
    U.foreach { case (id1, id2, mulVal) =>
      G(id1, id2) = mulVal
      G(id2, id1) = mulVal
    }
    idE.foreach { case (id, ent) =>
      G(id, id) = ent
    }

    Matrices.dense(n, n, G.data)
  }

  def mutInfor(inputRdd: RDD[ArrayBuffer[(Int, Int, String, String)]],
               oneEntropy: Map[Int, Double],
               otherEntropy: Map[Int, Double],
               sampleNum: Double): Array[(Int, Int, Double)] = {
    //result:((col1,col2), Num) for H(x,y)
    val joinNum = inputRdd
      .flatMap(x => x)
      .map(x => (x, 1l))
      .reduceByKey(_ + _)
      .map(x => ((x._1._1, x._1._2), -(x._2.toDouble / sampleNum) * Math.log(x._2.toDouble / sampleNum)))
      .reduceByKey(_ + _)

    //I(x,y) = H(x) + H(y) - H(x,y)
    val resultMulInfo = joinNum.map { case ((id1, id2), jEn) =>
      val mulInfo = oneEntropy.getOrElse(id1, 0.0) + otherEntropy.getOrElse(id2, 0.0) - jEn
      (id1, id2, mulInfo)
    }.collect()
    resultMulInfo
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

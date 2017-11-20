package com.tencent.angel.spark.ml.classification

import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.optim.FTRL
import com.tencent.angel.spark.ml.util.{ArgsUtil, ParamKeys}
import com.tencent.angel.spark.models.vector.{PSVector, SparsePSVector}
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, State, StreamingContext, Time}
import org.apache.spark.{SparkConf, SparkContext}

object SparseLRWithFTRL {

  //init param parameters for alpha, beta, lambda1, lambda2
  val ALPHA = "alpha"
  val BETA = "beta"
  val LAMBDA1 = "lambda1"
  val lAMBDA2 = "lambda2"
  val SEPARATOR = "separator"
  val CHECK_POINT_PATH = "checkPointPath"
  val SPACE_SPLITER = " "
  val ZK_QUORUM= "zkQuorum"
  val TOPIC = "topic"
  val GROUP = "group"
  val STREAMING_WINDOW = "streamingWindow"

  def main(args: Array[String]): Unit = {

    val params = ArgsUtil.parse(args)
    val alpha = params.getOrElse(ALPHA, "1.0").toDouble
    val beta = params.getOrElse(BETA, "1.0").toDouble
    val lambda1 = params.getOrElse(LAMBDA1, "1.0").toDouble
    val lambda2 = params.getOrElse(lAMBDA2, "1.0").toDouble
    val labelCol = params.getOrElse(ParamKeys.LABEL_COL, "0").toInt
    val dim = params.getOrElse("dim", "10").toInt
    val partitionNum = params.getOrElse(ParamKeys.PARTITION_NUM, "3").toInt
    val streamingWindow = params.getOrElse(STREAMING_WINDOW, "60").toInt
    val modelPath = params.getOrElse(ParamKeys.MODEL_PATH, null)
    val checkPointPath = params.getOrElse(CHECK_POINT_PATH, null)
    val zkQuorum = params.getOrElse(ZK_QUORUM, null)
    val topic = params.getOrElse(TOPIC, null)
    val group = params.getOrElse(GROUP, null)

    val sparkConf = new SparkConf().setAppName("SparseFTRLTest")
    val ssc = new StreamingContext(sparkConf, Seconds(streamingWindow))

    val sc = ssc.sparkContext

    PSContext.getOrCreate(sc)
    execute(ssc,
      modelPath,
      dim,
      alpha,
      beta,
      lambda1,
      lambda2,
      labelCol,
      partitionNum,
      checkPointPath,
      zkQuorum,
      topic,
      group)

  }

  def execute(ssc: StreamingContext,
              modelPath: String,
              dim: Int,
              alpha: Double,
              beta: Double,
              lambda1: Double,
              lambda2: Double,
              labelCol: Int,
              partitionNum: Int,
              checkPointPath: String,
              zkQuorum: String,
              topic: String,
              group: String
             ) = {

    ssc.checkpoint(checkPointPath)

    // for kafka mode
    val topicMap: Map[String, Int] = Map(topic -> 1)
    val featureDS = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)

    val zPS: SparsePSVector = PSVector.sparse(dim)
    val nPS: SparsePSVector = PSVector.sparse(dim)

    // 2: executor parallel to train the model
    trainOnExe(featureDS,
      zPS,
      nPS,
      dim,
      alpha,
      beta,
      lambda1,
      lambda2,
      labelCol,
      partitionNum,
      modelPath
    )

    // start to create the job
    ssc.start()
    // await for application stop
    ssc.awaitTermination()

  }

  // train the model on executors
  def trainOnExe(
                 featureDS: DStream[String],
                 zPS: SparsePSVector,
                 nPS: SparsePSVector,
                 dim: Int,
                 alpha: Double,
                 beta: Double,
                 lambda1: Double,
                 lambda2: Double,
                 labelCol: Int,
                 partitionNum: Int,
                 modelPath: String) = {

    featureDS.print()
    featureDS.foreachRDD { labelFeatRdd =>

      val incrementRdd = labelFeatRdd
        .repartition(partitionNum)
        .mapPartitionsWithIndex { (partitionId, dataIter) =>

          // init the increment of model for z and n
          var incrementZ: Map[Long, Double] = Map()
          var incrementN: Map[Long, Double] = Map()

          // pull the z and n model to executor
          var localZ = zPS.sparsePull().toMap
          var localN = nPS.sparsePull().toMap

          // compute the increment of z and n model
          println("before,z is:" + localZ.mkString(SPACE_SPLITER) + SPACE_SPLITER + partitionId)
          println("before,n is:" + localN.mkString(SPACE_SPLITER) + SPACE_SPLITER + partitionId)

          while (dataIter.hasNext) {

            val dataStr = dataIter.next()

            println("the dataStr is:" + dataStr)

            val labelFeature = dataStr.split(SPACE_SPLITER)
            val label = labelFeature(labelCol)
            val feature = labelFeature.tail
            val featureIdVal = feature.map{ idVal =>
              val idValArr = idVal.split(":")
              (idValArr(0).toLong, idValArr(1).toDouble)
            }

            val updateIncZN = FTRL.trainByInstance(
              (label, featureIdVal),
              localZ,
              localN,
              alpha,
              beta,
              lambda1,
              lambda2,
              getGredLoss
            )

            // add the increment of z and n model,z and n model will be change in this function
            incrementZ = incrementVector(incrementZ, updateIncZN._1)
            incrementN = incrementVector(incrementN, updateIncZN._2)

            localZ = incrementVector(localZ, updateIncZN._1)
            localN = incrementVector(localN, updateIncZN._2)
          }

          // test for increment of z and n model
          println("the increment of z is:" + incrementZ)
          println("the increment of n is:" + incrementN)

          // check the z and n models
          println("after, z is:" + localZ.mkString(SPACE_SPLITER))
          println("after, n is:" + localN.mkString(SPACE_SPLITER))

          // return the increment of z and n
          Iterator(("z" + partitionId, incrementZ), ("n" + partitionId, incrementN))
        }.cache()

      incrementRdd.count

      // push all the increment to z and n
      val increPush = incrementRdd.mapPartitions{ increZNIte =>

        while (increZNIte.hasNext) {
          val increZN = increZNIte.next()
          if (increZN._1.contains("z"))
            zPS.increment(increZN._2.toArray)
          else
            nPS.increment(increZN._2.toArray)
        }
        Iterator()
      }

      increPush.count()

      val modelSave = incrementRdd.mapPartitions{it =>
        val zModel = Array("z") ++ zPS.sparsePull().map(x => x._1 + ":" + x._2)
        val nModel = Array("n") ++ nPS.sparsePull().map(x => x._1 + ":" + x._2)
        println("last the model for z and n" + zModel.mkString(" "))
        Iterator(zModel ++ nModel)
      }
      save(modelSave, modelPath)
      incrementRdd.unpersist(false)

    }
  }

  // add updateInc to incrementedVec
  def incrementVector(incrementedVec: Map[Long, Double], updateInc: Array[(Long, Double)]): Map[Long, Double] = {

    var vecAddResult: Map[Long, Double] = incrementedVec

    updateInc.map { case (fId, fInc) =>
      val oriVal = vecAddResult.getOrElse(fId, 0.0)
      val newVal = oriVal + fInc
      vecAddResult += (fId -> newVal)
    }

    vecAddResult
  }

  def trackStateFunc(batchTime: Time, key: String, value: Option[Int], state: State[Long]): Option[(String, Long)] = {
    val sum = value.getOrElse(0).toLong + state.getOption.getOrElse(0L)
    val output = (key, sum)
    state.update(sum)
    Some(output)
  }

  // init the z and n model which is carry out on ps immediately
  def initZAndNModel(zPS: PSVector, nPS: PSVector, initValue: Double): (PSVector, PSVector) = {

    val zPSNew = zPS.toBreeze :* initValue
    val nPSNew = nPS.toBreeze :* initValue
    (zPSNew.component, nPSNew.component)
  }

  def save(dataRDD: RDD[Array[String]], path: String): Unit = {
    val outputPath = new Path(path)
    val conf = dataRDD.context.hadoopConfiguration

    val fs = outputPath.getFileSystem(conf)
    if (fs.exists(outputPath)) {
      fs.delete(outputPath, true)
    }

    dataRDD.map(_.mkString(SPACE_SPLITER)).saveAsTextFile(path)

  }

  // get the gradient of loss function
  def getGredLoss(w: Array[(Long, Double)],
                  label: Double,
                  feature: Array[(Long, Double)]): Map[Long, Double] = {
    val p = sigmoid(dot(w, feature))
    feature.map(x => (x._1, (p - label) * x._2)).toMap
  }

  // sigmoid function: 1 / (1 + exp(-z))
  def sigmoid(value: Double): Double = {
    1 / (1 + Math.exp(-1 * value))
  }

  private def dot(x: Array[(Long, Double)], y: Array[(Long, Double)]): Double = {
    val xValues = x.map(_._2)
    val xIndices = x.map(_._1)
    val yValues = y.map(_._2)
    val yIndices = y.map(_._1)
    val nnzx = xIndices.size
    val nnzy = yIndices.size

    var kx = 0
    var ky = 0
    var sum = 0.0
    // y catching x
    while (kx < nnzx && ky < nnzy) {
      val ix = xIndices(kx)
      while (ky < nnzy && yIndices(ky) < ix) {
        ky += 1
      }
      if (ky < nnzy && yIndices(ky) == ix) {
        sum += xValues(kx) * yValues(ky)
        ky += 1
      }
      kx += 1
    }
    sum
  }
}
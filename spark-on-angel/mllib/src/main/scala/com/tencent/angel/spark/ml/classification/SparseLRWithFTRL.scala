package com.tencent.angel.spark.ml.classification

import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.optim.FTRL
import com.tencent.angel.spark.ml.util.{ActionType, ArgsUtil, ParamKeys}
import com.tencent.angel.spark.models.vector.{PSVector, SparsePSVector}
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

class SparseLRWithFTRL extends Serializable{

  //init param parameters for alpha, beta, lambda1, lambda2
  private var alpha: Double = 1.0
  private var beta: Double = 1.0
  private var lambda1: Double = 1.0
  private var lambda2: Double = 1.0
  private var checkPointPath: String = _
  private var dim: Long = 0
  private var partitionNum: Int = 3
  private var modelPath: String = _
  val SPACE_SPLITTER: String = " "

  def setAlpha(alpha: Double): this.type = {
    this.alpha = alpha
    this
  }

  def setBeta(beta: Double): this.type = {
    this.beta = beta
    this
  }

  def setLambda1(lambda1: Double): this.type = {
    this.lambda1 = lambda1
    this
  }

  def setLambda2(lambda2: Double): this.type = {
    this.lambda2 = lambda2
    this
  }

  def setCheckPointPath(checkPointPath: String): this.type = {
    this.checkPointPath = checkPointPath
    this
  }

  def setDim(dim: Long): this.type = {
    this.dim = dim
    this
  }

  def setPartitionNum(partitionNum: Int): this.type = {
    this.partitionNum = partitionNum
    this
  }

  def setModelPath(modelPath: String): this.type = {
    this.modelPath = modelPath
    this
  }

  // train the model on executors
  def train(featureDS: DStream[String],
            dim: Long,
            alpha: Double,
            beta: Double,
            lambda1: Double,
            lambda2: Double,
            partitionNum: Int,
            modelPath: String) = {

    val zPS: SparsePSVector = PSVector.sparse(dim)
    val nPS: SparsePSVector = PSVector.sparse(dim)

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
          println("before,z is:" + localZ.mkString(SPACE_SPLITTER) + SPACE_SPLITTER + partitionId)
          println("before,n is:" + localN.mkString(SPACE_SPLITTER) + SPACE_SPLITTER + partitionId)

          while (dataIter.hasNext) {

            val dataStr = dataIter.next()

            println("the dataStr is:" + dataStr)

            val labelFeature = dataStr.split(SPACE_SPLITTER)
            val label = labelFeature(0)
            val feature = labelFeature.tail
            val featureIdVal = feature.map{ idVal =>
              val idValArr = idVal.split(":")
              (idValArr(0).toLong, idValArr(1).toDouble)
            }

            // compute the loss
            val predictVal = predictInstance(featureIdVal, localZ, localN, alpha, beta, lambda1, lambda2)
            println("the loss is:" + (predictVal - label.toDouble))

            // train
            val updateIncZN = FTRL.trainByInstance(
              (label, featureIdVal),
              localZ,
              localN,
              alpha,
              beta,
              lambda1,
              lambda2,
              getGradLoss
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
          println("after, z is:" + localZ.mkString(SPACE_SPLITTER))
          println("after, n is:" + localN.mkString(SPACE_SPLITTER))

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
        Iterator(zModel, nModel)
      }
      save(modelSave, modelPath)

      incrementRdd.unpersist(false)

    }
  }

  // add updateInc to incrementedVec
  def incrementVector(incrementedVec: Map[Long, Double], updateInc: Array[(Long, Double)]): Map[Long, Double] = {

    var vecAddResult: Map[Long, Double] = incrementedVec

    updateInc.foreach { case (fId, fInc) =>
      val oriVal = vecAddResult.getOrElse(fId, 0.0)
      val newVal = oriVal + fInc
      vecAddResult += (fId -> newVal)
    }

    vecAddResult
  }

  def predictInstance(feature: Array[(Long, Double)],
                      localZ: Map[Long, Double],
                      localN: Map[Long, Double],
                      alpha: Double,
                      beta: Double,
                      lambda1: Double,
                      lambda2: Double): Double = {

    val weight = getWeight(feature, localZ, localN, alpha, beta, lambda1, lambda2)
    // w * x
    sigmoid(dot(weight, feature))
  }

  def getWeight(feature: Array[(Long, Double)],
                localZ: Map[Long, Double],
                localN: Map[Long, Double],
                alpha: Double,
                beta: Double,
                lambda1: Double,
                lambda2: Double): Array[(Long, Double)] = {

    // the number of not zero of feature
    val featLeg = feature.length
    val localW = new Array[(Long, Double)](featLeg)

    (0 until featLeg).foreach { i =>

      val fId = feature(i)._1
      val zVal = localZ.getOrElse(fId, 0.0)
      val nVal = localN.getOrElse(fId, 0.0)
      // w_local的更新
      localW(i) = (fId, FTRL.updateWeight(zVal, nVal, alpha, beta, lambda1, lambda2))
    }

    localW
  }

  def save(dataRDD: RDD[Array[String]], path: String): Unit = {
    val outputPath = new Path(path)
    val conf = dataRDD.context.hadoopConfiguration

    val fs = outputPath.getFileSystem(conf)
    if (fs.exists(outputPath)) {
      fs.delete(outputPath, true)
    }

    dataRDD.map(_.mkString(SPACE_SPLITTER)).saveAsTextFile(path)

  }

  // get the gradient of loss function
  def getGradLoss(w: Array[(Long, Double)],
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
    val nnzx = xIndices.length
    val nnzy = yIndices.length

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

/**
  * this module is to run sparse lr with ftrl
  */

object SparseLRWithFTRL{

  //init param parameters for alpha, beta, lambda1, lambda2
  val ALPHA = "alpha"
  val BETA = "beta"
  val LAMBDA1 = "lambda1"
  val lAMBDA2 = "lambda2"
  val SEPARATOR = "separator"
  val CHECK_POINT_PATH = "checkPointPath"
  val SPACE_SPLITER = " "
  val ZK_QUORUM = "zkQuorum"
  val TOPIC = "topic"
  val GROUP = "group"
  val STREAMING_WINDOW = "streamingWindow"

  def main(args: Array[String]): Unit = {

    val params = ArgsUtil.parse(args)
    val actionType = params.getOrElse(ParamKeys.ACTION_TYPE, ActionType.TRAIN)
    val alpha = params.getOrElse(ALPHA, "1.0").toDouble
    val beta = params.getOrElse(BETA, "1.0").toDouble
    val lambda1 = params.getOrElse(LAMBDA1, "1.0").toDouble
    val lambda2 = params.getOrElse(lAMBDA2, "1.0").toDouble
    val dim = params.getOrElse("dim", "10").toLong
    val partitionNum = params.getOrElse(ParamKeys.PARTITION_NUM, "3").toInt
    val streamingWindow = params.getOrElse(STREAMING_WINDOW, "60").toInt
    val modelPath = params.getOrElse(ParamKeys.MODEL_PATH, null)
    val checkPointPath = params.getOrElse(CHECK_POINT_PATH, null)
    val zkQuorum = params.getOrElse(ZK_QUORUM, null)
    val topic = params.getOrElse(TOPIC, null)
    val group = params.getOrElse(GROUP, null)
    val input = params.getOrElse(ParamKeys.INPUT, null)
    val output = params.getOrElse(ParamKeys.OUTPUT, null)
    val sampleRate = params.getOrElse(ParamKeys.SAMPLE_RATE, "1.0").toDouble

    val sparseLR = new SparseLRWithFTRL()
      .setAlpha(alpha)
      .setBeta(beta)
      .setLambda1(lambda1)
      .setLambda2(lambda2)
      .setCheckPointPath(checkPointPath)
      .setDim(dim)
      .setPartitionNum(partitionNum)
      .setModelPath(modelPath)


    if(actionType == ActionType.TRAIN){
      val sparkConf = new SparkConf().setAppName("SparseFTRLTest")
      val ssc = new StreamingContext(sparkConf, Seconds(streamingWindow))
      val sc = ssc.sparkContext
      PSContext.getOrCreate(sc)
      ssc.checkpoint(checkPointPath)

      // for kafka mode
      val topicMap: Map[String, Int] = Map(topic -> 1)
      val featureDS = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)

      sparseLR.train(
        featureDS,
        dim,
        alpha,
        beta,
        lambda1,
        lambda2,
        partitionNum,
        modelPath)

      // start to create the job
      ssc.start()
      // await for application stop
      ssc.awaitTermination()

    }
    else{
      val sparkConf = new SparkConf().setAppName("SparseFTRLTest")
      val sc = SparkContext.getOrCreate(sparkConf)

      // parse the model of z and n
      val modelLocal = sc.textFile(modelPath).collect()
      val zModel = sparseModel(modelLocal, "z")
      val nModel = sparseModel(modelLocal, "n")

      val parseRDD = sc.textFile(input)
        .sample(false, sampleRate)
        .repartition(partitionNum)
        .map(line => line.trim)
        .filter(_.nonEmpty)
        .map{dataStr =>
          val labelFeature = dataStr.split(SPACE_SPLITER)
          val feature = labelFeature.tail
          val featureIdVal = feature.map { idVal =>
            val idValArr = idVal.split(":")
            (idValArr(0).toLong, idValArr(1).toDouble)
          }
          Array(dataStr, sparseLR.predictInstance(featureIdVal, zModel, nModel, alpha, beta, lambda1, lambda2).toString)
        }

      // save the predict rdd
      sparseLR.save(parseRDD, output)
//      parseRDD.foreach(x => println(x.mkString(" ")))
    }
  }

  // sparse the z and n model
  def sparseModel(model: Array[String], sign: String): Map[Long, Double] = {
    model.filter(str => str.contains(sign))(0)
      .split(SPACE_SPLITER)
      .tail
      .map{idVal =>
        val idValArr = idVal.split(":")
        (idValArr(0).toLong, idValArr(1).toDouble)
      }.toMap
  }

  def runSpark(name: String)(body: SparkContext => Unit): Unit = {
    println("this is in run spark")
    val conf = new SparkConf
    val master = conf.getOption("spark.master")
    val isLocalTest = if (master.isEmpty || master.get.toLowerCase.startsWith("local")) true else false
    val sparkBuilder = SparkSession.builder().appName(name)
    if (isLocalTest) {
      sparkBuilder.master("local")
        .config("spark.ps.mode", "LOCAL")
        .config("spark.ps.jars", "")
        .config("spark.ps.instances", "1")
        .config("spark.ps.cores", "1")
    }
    val sc = sparkBuilder.getOrCreate().sparkContext
    body(sc)
    val wait = sys.props.get("spark.local.wait").exists(_.toBoolean)
    if (isLocalTest && wait) {
      println("press Enter to exit!")
      Console.in.read()
    }
    sc.stop()
  }

}
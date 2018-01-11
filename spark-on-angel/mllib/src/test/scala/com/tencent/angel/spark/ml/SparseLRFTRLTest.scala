package com.tencent.angel.spark.ml

import java.util.Random

import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.linalg.SparseVector
import com.tencent.angel.spark.ml.online_learning.FTRLLearner
import com.tencent.angel.spark.models.vector.{PSVector, SparsePSVector}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object SparseLRFTRLTest {

  def main(args: Array[String]): Unit = {

    val topic = "ftrltopic"
    val zkQuorum = "localhost:2181"
    val group = "ftrltest"

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("FTRLTest")
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    ssc.checkpoint(".")

    val topicMap: Map[String, Int] = Map(topic -> 1)
    val featureDS = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)

    runSpark(this.getClass.getSimpleName) { sc =>
      PSContext.getOrCreate(sc)
      execute(ssc,featureDS, 11, 0.1, 1.0, 0.1, 0.1, 2, null, 0, 0, topic, zkQuorum, group, 0.3, 0.3, "ftrl")
    }

  }

  def execute(ssc: StreamingContext,
              featureDS: DStream[String],
              dim: Long,
              alpha: Double,
              beta: Double,
              lambda1: Double,
              lambda2: Double,
              partitionNum: Int,
              modelSavePath: String,
              batch2Check: Int,
              batch2Save: Int,
              topic: String,
              zkQuorum: String,
              group: String,
              rho1: Double,
              rho2: Double,
              method: String
             ) = {

    featureDS.print()

    if(method == "ftrl"){
      val zPS: SparsePSVector = PSVector.sparse(dim)
      val nPS: SparsePSVector = PSVector.sparse(dim)
      FTRLLearner.train(
        zPS,
        nPS,
        featureDS,
        dim,
        alpha,
        beta,
        lambda1,
        lambda2,
        partitionNum,
        modelSavePath,
        batch2Check,
        batch2Save)
    } else{
      val zPS: SparsePSVector = PSVector.sparse(dim)
      val nPS: SparsePSVector = PSVector.sparse(dim)
      val vPS: SparsePSVector = PSVector.sparse(dim)

      val initW = randomInit(dim)
      val initZInc = randomInit(dim).toArray

      println("random w is:" + initW.mkString(" "))
      println("random z is:" + initZInc.mkString(" "))

      zPS.increment(new SparseVector(dim, initZInc))

      FTRLLearner.train(zPS,
        nPS,
        vPS,
        initW,
        featureDS,
        dim,
        alpha,
        beta,
        lambda1,
        lambda2,
        rho1,
        rho2,
        partitionNum,
        modelSavePath,
        batch2Check,
        batch2Save)

    }

    ssc.start()
    ssc.awaitTermination()
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

  def randomInit(dim: Long): Map[Long, Double] = {
    val selectNum = if(dim < 10) dim.toInt else 10
    val dimReFact = if(dim <= Int.MaxValue ) dim.toInt else Int.MaxValue
    var resultRandom: Map[Long, Double] = Map()
    val randGene = new Random()

    (0 until selectNum).foreach { i =>
      val randomId = randGene.nextInt(dimReFact - 1).toLong
      val randomVal = 10 * randGene.nextDouble() + 1.0

      resultRandom += (randomId -> randomVal)
    }
    // indices 0 is not in our feature
    resultRandom.filter(x => x._1 > 0)
  }
}
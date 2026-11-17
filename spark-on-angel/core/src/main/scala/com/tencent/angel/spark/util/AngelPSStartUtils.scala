package com.tencent.angel.spark.util

import com.tencent.angel.AngelDeployMode
import com.tencent.angel.ps.EmbeddedParameterServer
import com.tencent.angel.spark.context.{AngelPSContext, PSContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{SparkListener, SparkListenerExecutorAdded}
import org.apache.spark._

object AngelPSStartUtils {

  private var startedRDD: RDD[Int] = _
  private var mode: String = "YARN"

  def startPS(sc: SparkContext, numPartitions: Int): Unit = {
    mode = sc.getConf.get("spark.ps.mode", AngelDeployMode.YARN.toString)
    if (mode.toUpperCase.equals(AngelDeployMode.EMBEDDED.toString)) {
      val numPs = sc.getConf.get("spark.executor.instances").toInt
      SparkEnv.get.conf.set("spark.ps.instances", numPs.toString)
      SparkEnv.get.conf.set("spark.ps.mode", AngelDeployMode.EMBEDDED.toString)
      println("num ps instances is " + numPs)
      val psContext = PSContext.getOrCreate(sc)
      import scala.collection.JavaConverters._
      val angelConf = new java.util.HashMap[String, String]()
      angelConf.putAll(psContext.asInstanceOf[AngelPSContext].conf.asJava)
      val angelMap = sc.broadcast(angelConf)
      val startRDD = sc.parallelize(1 to numPartitions, numPartitions).map(x => (x.toLong, 1))
      startedRDD = startRDD.partitionBy(new LongKeyHashPartitioner(numPartitions)).barrier().mapPartitions(iter => {
        // Start up Angel PS first
        val context = BarrierTaskContext.get()
        val attemptIndex = context.attemptNumber()
        val partitionId = context.partitionId()
        println("rdd partition id is: + " + partitionId + ", attemptIndex is: " + attemptIndex)
        //val psIndex = partitionId % numPs
        val psIndex = partitionId
        if (EmbeddedParameterServer.getInstance().run(angelMap.value)) {
          val executorId = SparkEnv.get.executorId
          val psId = EmbeddedParameterServer.getInstance().getPSId
          println(s"Start ps for executor ${executorId}, ps index is ${psId}")
        }
        context.barrier()
        Iterator.single(1)
      }, preservesPartitioning = true)
      startedRDD.cache()
      startedRDD.count()
      sc.addSparkListener(new ExecutorActionListener(startedRDD))
      // Wait all ps register
      AngelPSContext.waitAllPSReady()
    } else {
      PSContext.getOrCreate(sc)
    }
  }

  def stopPS(): Unit = {
    if (mode.toUpperCase.equals(AngelDeployMode.EMBEDDED.toString)) {
      // Stop ps
      startedRDD.barrier.mapPartitions(_ => {
        val context = BarrierTaskContext.get()
        EmbeddedParameterServer.getInstance().stop()
        context.barrier()
        Iterator.single(1)
      }).count()
    }

    // Stop angel master
    PSContext.stop()
  }

}

class ExecutorActionListener(val psCached: RDD[Int]) extends SparkListener {
  override def onExecutorAdded(executor: SparkListenerExecutorAdded): Unit = {
    println("Executor " + executor.executorId + " is added.")
    psCached.count()
  }
}

class LongKeyHashPartitioner(val partNum: Int) extends Partitioner {
  override def numPartitions: Int = partNum

  override def getPartition(key: Any): Int = {
    val longKey = key.asInstanceOf[Long]
    (longKey % partNum).toInt
  }
}

package com.tencent.angel.spark.ml.ann

import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.spark.ml.ann.utils.DataUtils
import com.tencent.angel.spark.ml.{PSFunSuite, SharedPSContext}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class ANNPsfTest extends PSFunSuite with SharedPSContext {
  
  val dataPath: String = testDataPath("ann/vectors.txt")
  var dataset: DataFrame = _
  var psModel: ANNPSModel = _
  var data: RDD[(Long, Array[Float])] = _
  var index: RDD[Long] = _
  
  override def beforeAll(): Unit = {
    super.beforeAll()
    
    println(s"load data and create ps model")
    val ss = SparkSession.builder().getOrCreate()
    
    dataset = DataUtils.loadVectors(ss, dataPath, 2)
  
    data = dataset.select("item", "vector").rdd
      .filter(row => !row.anyNull)
      .map(row => (row.getString(0).toLong, row.getString(1).split(" ").map(_.toFloat)))
      .repartition(2)
  
    data.persist(StorageLevel.MEMORY_ONLY)
    val numVectors = data.count()
  
    index = data.map(_._1)
    val minId = index.min()
    val maxId = index.max()
  
    val modelContext = new ModelContext(2, minId, maxId + 1, -1,
      SparkContext.getOrCreate().hadoopConfiguration)
    psModel = ANNPSModel.apply(modelContext, this.data)
  }
  
  override def afterAll(): Unit = super.afterAll()
  
  test("ANNVector PSF") {
    data.collect().sliding(50, 50).foreach { part =>
      psModel.initQueries(part)
    }
//    psModel.initVectors(data)
    
    println(s"pushed vectors onto ps")
    
    val vecs = psModel.getQueries(index.collect())
    
    data.collect().foreach{ dt =>
      assert(vecs.containsKey(dt._1), "not all vecs are pushed onto ps")
      assert(dt._2.length == vecs.get(dt._1).length)
      dt._2.zip(vecs.get(dt._1)).foreach{ pair =>
        assert(pair._1 == pair._2, s"changes in value of vecs on ps")
      }
      println(s"${dt._1}:${vecs.get(dt._1).mkString(" ")}")
    }
    
    println(s"succeed")
  }
  
  test("search result PSF") {
    val nodeIds = index.collect().sorted
    val sample = new ArrayBuffer[(Long, Array[(Long, Float)])]()
    for (i <- nodeIds) {
      val tres = new ArrayBuffer[(Long, Float)]()
      for (j <- 0 until 20) {
        tres += ((Random.nextInt(1000).toLong, (2 * j + 1).toFloat))
      }
      sample += ((i, tres.toArray))
    }
    
    psModel.updateSearchResult(sample.toArray, 10)
    
    val pulled = psModel.getNearestNeighbors(nodeIds, 20)
    for ((i, expected) <- sample) {
      assert(pulled.containsKey(i))
      assert(pulled.get(i).length == expected.length)
      for (pair <- expected.zip(pulled.get(i))) {
        assert(pair._1._1 == pair._2.getNeighbor && pair._1._2 == pair._2.getDistance,
          s"push res to ps changes the value, error")
      }
    }
  
    val sample2 = new ArrayBuffer[(Long, Array[(Long, Float)])]()
    for (i <- nodeIds) {
      val tres = new ArrayBuffer[(Long, Float)]()
      for (j <- 0 until 20) {
        tres += ((Random.nextInt(1000).toLong, (2 * j).toFloat))
      }
      sample2 += ((i, tres.toArray))
    }
    
    psModel.updateSearchResult(sample2.toArray, 10)
  
    val pulled2 = psModel.getNearestNeighbors(nodeIds, 10)
    for (i <- nodeIds) {
      assert(pulled2.containsKey(i))
      val distances = pulled2.get(i).map(_.getDistance)
      assert(distances.length == 10)
      assert(distances.sameElements((0 until 10).map(_.toFloat)))
    }
    
  }
  
  test("union self") {
    val rdd1 = sc.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8))
    val rdd2 = sc.parallelize(Array(100, 101, 102))
    
    val rddU1 = rdd1.union(rdd2)
    val rddU2 = rdd1.union(rdd1)
    
    println(s"${rddU1.collect().mkString(" ")}")
    println(s"${rddU2.collect().mkString(" ")}")
  
    rdd1.randomSplit(Array(1.0, 1.0)).foreach(t => println("0h" + t.collect().mkString(" ")))
    rdd1.randomSplit(Array(1.0)).foreach(t => println("1h" + t.collect().mkString(" ")))
    
  }
  
  
}

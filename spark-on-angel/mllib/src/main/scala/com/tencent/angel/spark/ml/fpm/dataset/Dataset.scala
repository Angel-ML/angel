package com.tencent.angel.spark.ml.fpm.dataset

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import scala.collection.mutable

object Dataset extends Serializable {
  def loadDataItems2(path: String, partitionNum: Int, sep: String = " ", reIndex: Boolean = false)
                  (implicit sc: SparkContext): (Option[Array[String]], DataFrame) = {
    val data = {
      sc.textFile(path).repartition(partitionNum).mapPartitions(iterator => {
        iterator.filter(r => !r.startsWith("#") || r.nonEmpty).map(_.trim)
      })
    
}
  
    val ss = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()
    import ss.implicits._

    val dataset = ss.createDataset(data).map(t => t.split(sep).map(_.toInt)).toDF("items")
    (None, dataset)
    
  }
  
  def loadDataItems(path: String, partitionNum: Int, sep: String = " ", reIndex: Boolean = false)
                   (implicit sc: SparkContext): (Option[Array[String]], DataFrame) = {
    val data = {
      sc.textFile(path).repartition(partitionNum).mapPartitions(iterator => {
        iterator.filter(r => !r.startsWith("#") || r.nonEmpty).map(_.trim)
      })
    
}
    
    println(s"data samples:")
    data.take(5).foreach( a => println(a))
    
    val ss = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()
    import ss.implicits._
    
    if (reIndex) {
      val itemsData = data.map(_.split(sep).distinct)
      
      val id2Item = itemsData.flatMap{ items =>
        items.flatMap(item => Iterator.single(item))
      }.distinct().collect()
      
      val item2Id = id2Item.zipWithIndex.toMap
      val broadItem2Id = sc.broadcast(item2Id)
      val reIndexedData = itemsData.mapPartitions { iter =>
        val itemMap = broadItem2Id.value
        iter.map{ transaction =>
          transaction.map(t => itemMap.getOrElse(t, -1))
        }
      }
      val dataset = ss.createDataset(reIndexedData).toDF("items")
      (Option[Array[String]](id2Item), dataset)
    }
    else {
      val dataset = ss.createDataset(data).map(t => t.split(sep).distinct.map(_.toInt)).toDF("items")
      (None, dataset)
    }
  }
  
  
  def save(df: DataFrame, output: String, seq: String = "\t"): Unit = {
    df.printSchema()
    ; {
      df.write
        .mode(SaveMode.Overwrite)
        .option("header", "false")
        .option("delimiter", seq)
        .csv(output)
    
}
  }
  

  

  
  def makeIntRepr(input: String, output: String, numWorker: Int)
                 (implicit sc: SparkContext): Unit = {
    val data = sc.textFile(input).repartition(numWorker).mapPartitions(iterator => {
      iterator.filter(r => !r.startsWith("#") || r.nonEmpty).map(_.trim)
    })
    val tmpMap = data.flatMap{ line =>
      line.split(" ")
    }.collect.distinct.zipWithIndex.toMap
    
    val bcMap = sc.broadcast(tmpMap)
    
    val res = data.map{ line =>
      line.split(" ").map(bcMap.value(_)).mkString(" ")
    }
    
    val schema = StructType(Seq(StructField("items", StringType, nullable = false)))
    val ss = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()
    import ss.implicits._
  
    val dataset = ss.createDataset(res).toDF("items")
  
    dataset.write
      .mode(SaveMode.Overwrite)
      .option("header", false)
      .option("delimiter", "\t")
      .csv(output)

  }
  
  def defaultCheckpointDir: Option[String] = {
    val sparkContext = SparkContext.getOrCreate()
    sparkContext.getConf.getOption("spark.yarn.stagingDir")
      .map { base =>
        new Path(base, s".sparkStaging/${sparkContext.getConf.getAppId}").toString
      }
  }
}

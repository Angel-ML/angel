package com.tencent.angel.spark.ml.classification

import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import com.tencent.angel.spark.linalg.{BLAS, OneHotVector, SparseVector}
import com.tencent.angel.spark.models.vector.cache.PullMan
import com.tencent.angel.spark.models.vector.{PSVector, SparsePSVector}

case class SparseLRModel(w: PSVector) {

  def sigmod(x: Double): Double = {
    1.0 / (1.0 + math.exp(-x))
  }

  def predict(instances: RDD[(Long, OneHotVector)]): RDD[(Long, Double)] = {
    val result = instances.mapPartitions { iter =>
      val localX = w.toCache.pullFromCache()
      iter.map { case (id, feature) =>
        val margin = BLAS.dot(localX, feature)
        Tuple2(id, sigmod(margin))
      }
    }
    PullMan.release(w)
    result
  }

  def predict(feat: OneHotVector, localW: SparseVector): Double = {
    val score = BLAS.dot(localW, feat)
    sigmod(score)
  }

  def save(modelPath: String): Unit = {
    w.toSparse.compress()
    val localX = w.pull.toSparse
    val keyValues = localX.keyValues
    assert(keyValues.defaultReturnValue() == 0.0)
    val iter = keyValues.long2DoubleEntrySet().fastIterator()

    val modelArray = new Array[String](keyValues.size())
    var i = 0
    while (iter.hasNext) {
      val entry = iter.next()
      modelArray(i) = f"${entry.getLongKey} : ${entry.getDoubleValue}%.6f"
      i += 1
    }
    val spark = SparkSession.builder().getOrCreate()

    val path = new Path(modelPath)
    val fs = path.getFileSystem(spark.sparkContext.hadoopConfiguration)
    if (fs.exists(path)) fs.delete(path, true)

    spark.sparkContext.makeRDD(modelArray).saveAsTextFile(modelPath + "/data")
    val meta = Array(w.dimension.toString)
    spark.sparkContext.makeRDD(meta, 1).saveAsTextFile(modelPath + "/meta")
    println(s"save model finished")
  }

  def simpleInfo: String = {
    val localW = w.pull.toSparse
    s"weight nnz: ${localW.nnz} some index and value: ${localW.indices.slice(0, 10).mkString("[", "," ,"]")}" +
      s" => ${localW.values.slice(0, 10).mkString("[", "," ,"]")}"
  }
}

object SparseLRModel {
  def load(modelPath: String): SparseLRModel = {
    val spark = SparkSession.builder().getOrCreate()
    val dim = spark.sparkContext.textFile(modelPath + "/meta")
      .collect()(0).toLong

    val modelArray = spark.sparkContext.textFile(modelPath + "/data")
      .map { line =>
        val items = line.split(":")
        require(items.length == 2, "each line of model must be \"featureId:value\" format")
        Tuple2(items(0).toLong, items(1).toDouble)
      }.collect()
    val keyValues = new Long2DoubleOpenHashMap(modelArray.length)
    modelArray.foreach { case (key, value) => keyValues.put(key, value) }
    println(s"load data success, dim: $dim, model nnz: ${keyValues.size()}")
    val localX = new SparseVector(dim, keyValues)
    val psX = SparsePSVector.apply(localX.length, 20)
    psX.push(localX)
    println(s"load model successfully")
    SparseLRModel(psX)
  }
}
package com.tencent.angel.spark.examples.cluster

import java.util.Random
import com.tencent.angel.spark.ml.core.{ArgsUtil => coreArgsUtil}
import com.tencent.angel.spark.ml.util.{DataLoader, DataSaver, LogUtils}
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import com.tencent.angel.spark.ml.featureEngineering.Dummy._
import scala.collection.mutable.ArrayBuffer

object DummyExample {

  val SEP_INSIDE_FEATURE = "_"
  val SEP_MULTI_FEATURE = "\\|"
  val SEP_OUTPUT = ","
  val SEP_FEAT_INDEX = ":"
  val TARGET_NAME = "target"
  val INSTANCE_DIR = "/instances"
  val FEATURE_INDEX_DIR = "/featureIndexs"
  val FEATURE_COUNT_DIR = "/featureCount"

  val rand = new Random(System.currentTimeMillis())

  private def isValidValue(item: String): Boolean = {
    item.nonEmpty && item != "0" && item != "-1"
  }

  def toInstance(fields: Array[String],
                 idFeatures: Array[(String, Int)],
                 combFeatures: Array[(String, Array[Int])],
                 matchInfoMap: Map[String, Int],
                 negSampleRate: Double): Iterator[(String, Array[String])] = {

    val featureSize = matchInfoMap.size

    if (fields.length < featureSize) {
      return Iterator.empty
    }

    val targetIndex = if (matchInfoMap.contains(TARGET_NAME)) {
      // dense data
      matchInfoMap(TARGET_NAME)
    } else {
      // libsvm data
      0
    }

    val target = fields(targetIndex)
    if (target.isEmpty) {
      return Iterator.empty
    }
    // 负样本采样
    if (target == "0" && negSampleRate > 0 && negSampleRate < 1.0
      && rand.nextInt(10000) > 10000 * negSampleRate) {
      return Iterator.empty
    }

    val feature1 = idFeatures.flatMap { case (featureName, index) =>
      val featValue = fields(index)
      if (isValidValue(featValue)) {
        featValue.split(SEP_MULTI_FEATURE).map(_.trim)
          .filter(item => isValidValue(item))
          .map(item => featureName + SEP_INSIDE_FEATURE + item)
          .toIterator
      } else {
        Iterator.empty
      }
    }

    val feature2 = new ArrayBuffer[String]()
    combFeatures.foreach(f => {
      val featureName = f._1
      val dependencies = f._2
      addCombFeature(feature2, featureName, 0, dependencies, fields)
    })

    val instance = Tuple2(target, (feature1.toIterator ++ feature2.toIterator).toArray)
    Iterator.single(instance)
  }

  private def addCombFeature(features: ArrayBuffer[String],
                             prefix: String,
                             index: Int,
                             dependencies: Array[Int],
                             fields: Array[String]) {
    if (index >= dependencies.length - 1) {
      fields(dependencies(index)).split(SEP_MULTI_FEATURE)
        .filter(isValidValue)
        .foreach(f =>
          features.append(prefix + SEP_INSIDE_FEATURE + f)
        )
    } else {
      fields(dependencies(index)).split(SEP_MULTI_FEATURE)
        .filter(isValidValue)
        .foreach(f =>
          addCombFeature(features, prefix + SEP_INSIDE_FEATURE + f, index + 1, dependencies, fields)
        )
    }
  }

  def crossFeature(inputRDD: RDD[Array[String]],
                   featConfPath: String,
                   negSampleRate: Double): RDD[(String, Array[String])] = {

    val featConf = FeatureConf(featConfPath)

    val idFeatures = featConf.idFeatures
    val combFeatures = featConf.combFeature
    val matchInfoMap = featConf.field2IndexMap

    LogUtils.logTime(s"feature field count: ${matchInfoMap.size}")
    LogUtils.logTime("idFeatures length: " + idFeatures.length)
    LogUtils.logTime("combFeatures length: " + combFeatures.length)

    inputRDD.flatMap { feats =>
      toInstance(feats, idFeatures, combFeatures, matchInfoMap, negSampleRate)
    }
  }

  def save(output: String, instanceDir: String, indexDir: String, countDir: String,
           featureIndex: RDD[(String, Int)],
           oneHotInstance: RDD[(String, Array[Int])], sep: String): Unit = {

    val instancePath = if (instanceDir != null && instanceDir.nonEmpty)
      instanceDir else output + INSTANCE_DIR
    val featurePath = if (indexDir != null && indexDir.nonEmpty)
      indexDir else output + FEATURE_INDEX_DIR
    val maxDimPath = if (countDir != null && countDir.nonEmpty)
      countDir else output + FEATURE_COUNT_DIR

    val conf = featureIndex.context.hadoopConfiguration

    for (path <- Array(instancePath, featurePath, maxDimPath)) {
      val fs = new Path(path).getFileSystem(conf)
      if (fs.exists(new Path(path))) {
        fs.delete(new Path(path), true)
      }
      fs.close()
    }

    featureIndex.map { case (featName, index) => featName + SEP_FEAT_INDEX + index }
      .saveAsTextFile(featurePath)

    /*
    oneHotInstance.map { case (target, feat) =>
      target + SEP_OUTPUT + feat.mkString(SEP_OUTPUT)
    }.saveAsTextFile(instancePath)
    */

    featureIndex.context.parallelize(Seq(featureIndex.count()), 1)
      .saveAsTextFile(maxDimPath)

    // save one-hot instances
    val instancesResult = oneHotInstance.map { case (target, feat) =>
      Array(target) ++ feat.map(_.toString)
    }

    DataSaver.save(instancesResult, instancePath, sep)
  }

  def loadFeatIndex(featIndexPath: String): RDD[(String, Int)] = {
    val sc = SparkContext.getOrCreate()
    sc.textFile(featIndexPath)
      .map { line =>
        val items = line.split(SEP_FEAT_INDEX)
        (items(0), items(1).toInt)
      }
  }

  def cleanCheckpoint(checkPointDir: String): Unit = {
    val sc = SparkContext.getOrCreate()
    val checkPointPath = new Path(checkPointDir)
    val fs = checkPointPath.getFileSystem(sc.hadoopConfiguration)
    if (fs.exists(checkPointPath)) {
      fs.delete(checkPointPath, true)
    }
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

  def main(args: Array[String]) {
    LogUtils.logTime(s"Start to process Dummy...")

    val params = coreArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn-cluster")
    val input = params.getOrElse("input", null)
    val output = params.getOrElse("output", null)
    val partitionNum = params.getOrElse("partitionNum", "10").toInt
    val sep = params.getOrElse("sep", "space") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
    }

    val baseFeatIndexPath = params.getOrElse("baseFeatIndexPath", null)
    val featConfPath = params.getOrElse("user-files", null)


    val negSampleRate = params.getOrElse("negSampleRate", "1.0").toDouble
    val countThreshold = params.getOrElse("countThreshold", "5").toInt
    val instanceDir = params.getOrElse("instanceDir", null)
    val indexDir = params.getOrElse("indexDir", null)
    val countDir = params.getOrElse("countDir", null)

    val sc = start(mode)

    // load data
    val inputRDD = DataLoader.loadTable(sc, input, partitionNum, 1.0, null, sep)
      .rdd.map { row => row.toSeq.map(_.toString).toArray }
    LogUtils.logTime(s"input data count: ${inputRDD.count()}")

    // do cross
    val instanceRDD = crossFeature(inputRDD, featConfPath, negSampleRate)
    LogUtils.logTime(s"instance count: ${instanceRDD.count}")

    // load base feature index
    val (featureIndex, oneHotInstance) = if (baseFeatIndexPath != null) {
      val baseFeatIndexRDD = loadFeatIndex(baseFeatIndexPath)
      OneHotEncoder.processOneHot(instanceRDD, countThreshold, baseFeatIndexRDD)
    } else {
      OneHotEncoder.processOneHot(instanceRDD, countThreshold)
    }
    save(output, instanceDir, indexDir, countDir, featureIndex, oneHotInstance, sep)

    LogUtils.logTime(s"feature count: ${featureIndex.count()} instance count: ${oneHotInstance.count()}")


    LogUtils.logTime("ml.feature.num" + featureIndex.count().toString)
    LogUtils.logTime(s"Dummy finished.")
    stop()
  }
}

package org.apache.spark.ml.recommendation.angel


import java.text.SimpleDateFormat
import java.util.Date

import com.tencent.angel.spark.ml.core.{ArgsUtil => coreArgsUtil}
import com.tencent.angel.spark.ml.graph.utils.GraphIO
import com.tencent.angel.spark.ml.util.{BLAS, _}
import org.apache.spark.ml.recommendation.ALS.Rating
import org.apache.spark.ml.recommendation.{ALSModel, TopByKeyAggregator, ALS => MLALS}
import org.apache.spark.ml.util.{Identifiable, SchemaUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.BoundedPriorityQueue
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by isakjiang on 2021/09/22.
 */
class ALS extends Learner with Serializable {
  private var userCol = 0
  private var itemCol = 1
  private var ratingCol = 2
  private var partitionNum = 100
  private var sampleRate = 1.0

  private var rank = 5
  private var regParam = 0.1
  private var maxIter = 20
  private var implicitPrefs = false
  private var nonNegative = false
  private var alpha = 1.0

  private var recommendPath: String = null
  private var recommendNum: Int = 5
  private var recommendForUser: Boolean = true
  private var blockSize: Int = 4096
  private var checkpointInterval = 10

  private var featureColExpr: String = null
  private var sep = " "
  private var outputUserDF: String = null
  private var outputItemDF: String = null

  def setOutputUserDF(outputUserDF: String): this.type = {
    this.outputUserDF = outputUserDF
    this
  }

  def setOutputItemDF(outputItemDF: String): this.type = {
    this.outputItemDF = outputItemDF
    this
  }

  def setSep(sep: String): this.type = {
    this.sep = sep
    this
  }

  //new feature selection methods
  def setFeatureCol(cols: String): this.type = {
    this.featureColExpr = cols
    this
  }

  def setUserCol(col: Int): this.type = {
    this.userCol = col
    this
  }

  def setItemCol(col: Int): this.type = {
    this.itemCol = col
    this
  }

  def setRatingCol(col: Int): this.type = {
    this.ratingCol = col
    this
  }

  def setPartitionNum(num: Int): this.type = {
    this.partitionNum = num
    this
  }

  def setSampleRate(rate: Double): this.type = {
    this.sampleRate = rate
    this
  }

  def setRank(rank: Int): this.type = {
    this.rank = rank
    this
  }

  def setRegParam(regParam: Double): this.type = {
    this.regParam = regParam
    this
  }

  def setMaxIter(maxIter: Int): this.type = {
    this.maxIter = maxIter
    this
  }

  def setImplicitPrefs(implicitPrefs: Boolean): this.type = {
    this.implicitPrefs = implicitPrefs
    this
  }

  def setNonNegative(nonNegative: Boolean): this.type = {
    this.nonNegative = nonNegative
    this
  }

  def setAlpha(alpha: Double): this.type = {
    this.alpha = alpha
    this
  }

  def setRecommendPath(path: String): this.type = {
    this.recommendPath = path
    this
  }

  def setRecommendForUser(isUser: Boolean): this.type = {
    this.recommendForUser = isUser
    this
  }

  def setRecommendNum(num: Int): this.type = {
    this.recommendNum = num
    this
  }

  def setBlockSize(blockSize: Int): this.type = {
    this.blockSize = blockSize
    this
  }

  def setCheckpointInterval(checkpointInterval: Int): this.type = {
    this.checkpointInterval = checkpointInterval
    this
  }

  override def train(input: String, testSet: String): Model = {
    val sc = SparkContext.getOrCreate()
    val inputDF = DataLoader.loadTable(sc, input, partitionNum, sampleRate, featureColExpr, sep)
    val oriSchema = inputDF.schema
    val userColName = oriSchema.fieldNames(userCol)
    val itemColName = oriSchema.fieldNames(itemCol)
    val ratingColName = oriSchema.fieldNames(ratingCol)

    LogUtils.logTime(s"Input record count: ${inputDF.count()}")
    inputDF.printSchema()


    val conf = sc.getConf
    conf.getOption("spark.ml.checkpoint.interval").map(_.toInt).foreach { interval =>
      val checkpointRoot = sc.getConf
        .get("spark.ml.checkpoint.dir", conf.get("spark.hadoop.fs.defaultFS") + "/tmp")
      val checkpointDir = s"$checkpointRoot/spark/${
        new SimpleDateFormat("yyyyMMdd").format(new Date())
      }/${sc.applicationId}/checkpoint"
      LogUtils.logTime(s"set checkpoint dir: $checkpointDir")
      sc.setCheckpointDir(checkpointDir)
      setCheckpointInterval(interval)
    }

    val ratings = inputDF
      .select(
        col(userColName).cast(LongType),
        col(itemColName).cast(LongType),
        col(ratingColName).cast(FloatType)
      ).rdd.map { row =>
      Rating(row.getLong(0), row.getLong(1), row.getFloat(2))
    }

    val (userFactors, itemFactors) = MLALS.train(ratings,
      rank = rank,
      maxIter = maxIter,
      regParam = regParam,
      implicitPrefs = implicitPrefs,
      alpha = alpha,
      nonnegative = nonNegative,
      numUserBlocks = partitionNum,
      numItemBlocks = partitionNum,
      checkpointInterval = checkpointInterval
    )

    userFactors.persist(StorageLevel.MEMORY_AND_DISK)
    itemFactors.persist(StorageLevel.MEMORY_AND_DISK)
    userFactors.count()
    itemFactors.count()

    //switch to df
    val ss = SparkSession.builder().getOrCreate()
    import ss.implicits._
    val userDF = userFactors.toDF("id", "features")
    val itemDF = itemFactors.toDF("id", "features")

    val uid = Identifiable.randomUID("als")
    val model = new ALSModel(uid, rank, userDF, itemDF)

    if (recommendPath != null) {
      recommend(model)
    }

    if (outputUserDF != null && outputUserDF != "") {
      val userFactorsOutput = userFactors.map { item => (item._1, item._2.mkString("|")) }.toDF("id", "features")
      GraphIO.save(userFactorsOutput, outputUserDF, sep)
    }

    if (outputItemDF != null && outputItemDF != "") {
      val itemFactorsOutput = itemFactors.map { item => (item._1, item._2.mkString("|")) }.toDF("id", "features")
      GraphIO.save(itemFactorsOutput, outputItemDF, sep)
    }

    new MLModel(model)
  }

  override def predict(input: String, output: String, model: Model) {
    val sc = SparkContext.getOrCreate()
    val oriDF = DataLoader.loadTable(sc, input, partitionNum, sampleRate, featureColExpr, sep)
    LogUtils.logTime(s"Predict data size: ${oriDF.count()}")

    val oriSchema = oriDF.schema
    val userColName = oriSchema.fields(userCol).name
    val itemColName = oriSchema.fields(itemCol).name

    val userDF = oriDF.withColumn(userColName, oriDF(userColName).cast(LongType))
    val predictDF = userDF.withColumn(itemColName, userDF(itemColName).cast(LongType))

    val alsModel = model.asInstanceOf[MLModel].model.asInstanceOf[ALSModel]

    alsModel.setUserCol(userColName)
      .setItemCol(itemColName)
      .setPredictionCol("predict")
      .setColdStartStrategy("drop")

    val result = transform(alsModel, predictDF)

    result.printSchema()
    DataSaver.save(result, output, sep)
  }

  override def loadModel(modelPath: String): Model = {
    MLModel.loadFrom(modelPath, ALSModel)
  }

  private val predict = udf { (featuresA: Seq[Float], featuresB: Seq[Float]) =>
    if (featuresA != null && featuresB != null) {
      // TODO(SPARK-19759): try dot-producting on Seqs or another non-converted type for
      // potential optimization.
      import com.github.fommil.netlib.BLAS.{getInstance => blas}
      blas.sdot(rank, featuresA.toArray, 1, featuresB.toArray, 1)
    } else {
      Float.NaN
    }
  }

  def transform(alsModel: ALSModel, dataset: DataFrame): DataFrame = {
    val schema = dataset.schema
    val userColName = schema.fields(userCol).name
    val itemColName = schema.fields(itemCol).name
    val predictColName = "prediction"
    SchemaUtils.appendColumn(schema, predictColName, FloatType)

    // create a new column named map(predictionCol) by running the predict UDF.
    val predictions = dataset
      .join(alsModel.userFactors,
        dataset(userColName) === alsModel.userFactors("id"), "left")
      .join(alsModel.itemFactors,
        dataset(itemColName) === alsModel.itemFactors("id"), "left")
      .select(dataset("*"),
        predict(alsModel.userFactors("features"), alsModel.itemFactors("features")).as(predictColName))

    predictions.na.drop("all", Seq(predictColName))
  }

  private def blockify(
                        factors: Dataset[(Long, Array[Float])],
                        blockSize: Int = 4096): Dataset[Seq[(Long, Array[Float])]] = {
    import factors.sparkSession.implicits._
    factors.mapPartitions(_.grouped(blockSize))
  }

  def recommend(alsModel: ALSModel): Unit = {
    val recommendRDD = if (recommendForUser) {
      recommendForAll(alsModel.userFactors, alsModel.itemFactors,
        alsModel.userCol.name, alsModel.itemCol.name, recommendNum, blockSize)
    } else {
      recommendForAll(alsModel.itemFactors, alsModel.userFactors,
        alsModel.itemCol.name, alsModel.userCol.name, recommendNum, blockSize)
    }

    import alsModel.userFactors.sparkSession.implicits._

    val rdd = recommendRDD.map { i =>
      val recStr = i._2.map { rec => rec._1.toString + ":" + rec._2.toString }.mkString("|")
      (i._1.toString, recStr)
    }.toDF("id","recStr").repartition(partitionNum)

    LogUtils.logTime(s"rdd count: ${rdd.count()}")
    GraphIO.save(rdd, recommendPath, sep)
  }

  def recommendForAll(
                       srcFactors: DataFrame,
                       dstFactors: DataFrame,
                       srcOutputColumn: String,
                       dstOutputColumn: String,
                       num: Int,
                       blockSize: Int = 4096): RDD[(Long, Seq[(Long, Float)])] = {
    import srcFactors.sparkSession.implicits._

    val srcFactorsBlocked = blockify(srcFactors.as[(Long, Array[Float])], blockSize)
    val dstFactorsBlocked = blockify(dstFactors.as[(Long, Array[Float])], blockSize)

    val ratings = srcFactorsBlocked.crossJoin(dstFactorsBlocked)
      .as[(Seq[(Long, Array[Float])], Seq[(Long, Array[Float])])]
      .flatMap { case (srcIter, dstIter) =>
        val m = srcIter.size
        val n = math.min(dstIter.size, num)
        val output = new Array[(Long, Long, Float)](m * n)
        var i = 0
        val pq = new BoundedPriorityQueue[(Long, Float)](num)(Ordering.by(_._2))
        srcIter.foreach { case (srcId, srcFactor) =>
          dstIter.foreach { case (dstId, dstFactor) =>
            // We use F2jBLAS which is faster than a call to native BLAS for vector dot product
            val score = BLAS.f2jBLAS.sdot(rank, srcFactor, 1, dstFactor, 1)
            pq += dstId -> score
          }
          pq.foreach { case (dstId, score) =>
            output(i) = (srcId, dstId, score)
            i += 1
          }
          pq.clear()
        }
        output.toSeq
      }

    ratings.persist(StorageLevel.MEMORY_AND_DISK)

    // We'll force the IDs to be Int. Unfortunately this converts IDs to Int in the output.
    val topKAggregator = new TopByKeyAggregator[Long, Long, Float](num, Ordering.by(_._2))
    val recs = ratings.as[(Long, Long, Float)].groupByKey(_._1).agg(topKAggregator.toColumn)
      .toDF("id", "recommendations")

    val arrayType = ArrayType(
      new StructType()
        .add(dstOutputColumn, LongType)
        .add("rating", FloatType)
    )
    recs.select($"id".as(srcOutputColumn), $"recommendations".cast(arrayType))
      .as[(Long, Seq[(Long, Float)])].rdd

  }

}

/**
 * Alternating Least Squares (ALS) matrix factorization
 *
 * 参数说明:
 *
 * -----input parameter-----
 * input          input path
 * userCol        user列(从0开始计数), default: 0
 * itemCol        item列(从0开始计数), default: 1
 * ratingCol      rating列(从0开始计数), default: 2
 * partitionNUm     partition数目, 并行数
 * sampleRate     采样率. 默认:1.0 [optional]
 *
 * -----algorithm parameter----
 * rank
 * regParam       正则项系数
 * maxIter        最大迭代次数
 *
 * -----system parameter-----
 * mode          local or yarn-cluster
 * actionType    train/predict
 * modelPath     model HDFS path. train阶段, 模型保存到该路径, predict阶段, 从该路径load模型
 * testSet       testSet path for evaluate trained model. if null, then skip test step
 * output        预测的输出路径, 在train阶段, output == null
 */

object ALS {

  def main(args: Array[String]): Unit = {
    LogUtils.logTime(s"Start Angel ALS Processing...")
    val params = coreArgsUtil.parse(args)

    val mode = "local"
    val input = "data/mlTestData/u.data"
    val userCol = 0
    val itemCol = 1
    val ratingCol = 2
    val sampleRate = 1
    val partitionNum = 1

    val rank = 3
    val regParam = 0.1
    val maxIter = 20
    val implicitPrefs = false
    val nonNegative = false
    val alpha = 1

    val recommendPath = "data/output/outputRecomm"
    val userOrItem = "user"
    val recommendNum = 5
    val blockSize = 4096

    val actionType = "train"
    val modelPath = "data/output/modelPath"
    val testSetPath = null
    val output = "data/output/output"
    val outputUserDF = "data/output/outputUserDF"
    val outputItemDF = "data/output/outputItemDF"

    val sep = "\t"

    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster(mode)
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    //    ss.sparkContext.setCheckpointDir()

    val als = new ALS()
      .setUserCol(userCol)
      .setItemCol(itemCol)
      .setRatingCol(ratingCol)
      .setPartitionNum(partitionNum)
      .setSampleRate(sampleRate)
      .setRank(rank)
      .setRegParam(regParam)
      .setMaxIter(maxIter)
      .setImplicitPrefs(implicitPrefs)
      .setNonNegative(nonNegative)
      .setAlpha(alpha)
      .setRecommendPath(recommendPath)
      .setRecommendNum(recommendNum)
      .setRecommendForUser(if (userOrItem == "user") true else false)
      .setBlockSize(blockSize)
      .setSep(sep)
      .setOutputItemDF(outputItemDF)
      .setOutputUserDF(outputUserDF)

    als.process(actionType, input, modelPath, testSetPath, output)
    LogUtils.logTime(s"${this.getClass.getSimpleName} finished.")
  }
}

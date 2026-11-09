package com.tencent.angel.spark.ml.clustering.kmeans

import com.tencent.angel.spark.ml.util._
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.hadoop.fs.Path
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.jpmml.model.MetroJAXBUtil
import org.jpmml.sparkml.PMMLBuilder

/**
 * KMeans聚类算法
 *
 * 参数说明:
 *
 * mode          "local" or yarn-cluster
 * input         input file
 * featureCols      表示需要计算的特征所在列，例如“1-10,12,15”，其说明取特征在表中的第1到第10列，第12列以及第15列，从0开始计数
 * partitionNum   partiion数目
 * sampleRate   采样率. 默认:1.0 [optional]
 * output        聚类结果输出路径
 * centersOutput 聚类中心输出路径
 *
 * k             k-means聚类个数k
 * initMode      k-means聚类中心初始化模式. 默认:k-means|| [optional:"random" or "k-means||"]
 * maxIter       最大迭代次数. 默认:20 [optional]
 * epsilon       最小终止条件:累计误差. 默认:1e-4 [optional]
 */


object KMeansMLLocal {
  val K_KEY = "k"
  val INIT_MODE_KEY = "initMode"

  def main(args: Array[String]) {
    LogUtils.logTime("KMeans clustering start...")

    val mode = "local"
    /*
    val input = "data/mlTestData/Iris.csv"
    val featureCols = "0-3"
    val sep = ","

     */
    val input = "data/mlTestData/data_test_libsvm.txt" //xxxx.txt,poker_hand.csv
    val featureCols = null//"1-20"
    val sep = " "

    val sampleRate = 1
    val partitionNum = 1
    val output = "spark-on-angel/mllib/src/main/scala/com/tencent/angel/spark/ml/featurePreprocess/MLDataSetOutput/outKMeans"
    val centersOutput = "spark-on-angel/mllib/src/main/scala/com/tencent/angel/spark/ml/featurePreprocess/MLDataSetOutput/outKMeansCenters"
    val modelPath = "spark-on-angel/mllib/src/main/scala/com/tencent/angel/spark/ml/featurePreprocess/MLDataSetOutput/KMModel"
    val PMMLmodelPath = "spark-on-angel/mllib/src/main/scala/com/tencent/angel/spark/ml/featurePreprocess/MLDataSetOutput/KMModel_pmml"

    // for k-means algorithm
    val k = 3
    val initMode = "random" // "random" or "k-means||"
    val actionType = "train" //"predict"//"train"

    val maxIteration = 10
    val epsilon = 1e-4
    val needClusteringEvaluator = true
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster(mode)
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val clusterResult = run(input,
      featureCols,
      partitionNum,
      k,
      sampleRate,
      initMode,
      maxIteration,
      epsilon,
      sc,
      sep,
      centersOutput,
      modelPath,
      actionType,
      needClusteringEvaluator,
      PMMLmodelPath
    )
    DataSaver.save(clusterResult, output, sep)
  }

  /**
   * 调用聚类算法
   *
   * @param input
   * @param partitionNum
   * @param k
   * @param sampleRate
   * @param initMode
   * @param maxIteration
   * @param epsilon
   * @param sc
   * @return
   */
  def run(input: String,
          featureCols: String,
          partitionNum: Int,
          k: Int,
          sampleRate: Double,
          initMode: String,
          maxIteration: Int,
          epsilon: Double,
          sc: SparkContext,
          sep: String,
          centersOutput: String,
          modelPath: String,
          actionType: String,
          needClusteringEvaluator: Boolean,
          PMMLmodelPath: String
         ): DataFrame = {

    val model = if (actionType == "train") {
      val inputDF = DataLoader.loadTableWithLabel(sc, input, partitionNum, sampleRate, -1, featureCols, sep)
      var featuresCols = inputDF.columns
      if(featuresCols(0)=="id") featuresCols = featuresCols.drop(1)
      val trainingDFtable = inputDF.select(featuresCols.map(name => col(name).cast(DoubleType)): _*)
      val assembler: VectorAssembler = new VectorAssembler().setInputCols(featuresCols).setOutputCol("feature")

      println("---------- dataset examples :----------")
      trainingDFtable.show(5)
      val km = new KMeans()
        .setK(k)
        .setMaxIter(maxIteration)
        .setTol(epsilon)
        .setFeaturesCol(DFStruct.FEATURE)
        .setInitMode(initMode)

      val pipeline = new Pipeline().setStages(Array(assembler, km))
      val pipelineModel = pipeline.fit(trainingDFtable)
      println("success fit......")


      val pmml = new PMMLBuilder(trainingDFtable.schema, pipelineModel).build()
      if (PMMLmodelPath != "" && PMMLmodelPath != null) {
        val baos = new ByteArrayOutputStream()
        MetroJAXBUtil.marshalPMML(pmml, baos)
        val ss = trainingDFtable.sparkSession
        import ss.implicits._
        val writer = ss.createDataset(Seq(baos.toString)).write
        writer.mode(SaveMode.Overwrite)
        val savePath = new Path(PMMLmodelPath)
        writer.text(savePath.toString)
      }
      pipelineModel.stages(1).asInstanceOf[KMeansModel]

    }
    else {
      println("load KMeans model ...")
      KMeansModel.load(modelPath)
    }

    if (actionType == "train" && modelPath != null) {
      model.write.overwrite().save(modelPath)
    }


    val inputDFForpredict = DataLoader.loadUnlabeled(sc, input, partitionNum, sampleRate, featureCols, false, sep)
    val predictions = model.transform(inputDFForpredict)

    /*
     LogUtils.logTime("KMeans Cluster center:")
     for (center <- model.clusterCenters) {
       LogUtils.logTime(center.toString)
     }
     */

    if (actionType == "train" && centersOutput != null) {
      val centerRDD = sc.parallelize(model.clusterCenters.map(a => a.toArray.map(b => b.toString)))
      DataSaver.save(centerRDD, centersOutput, sep)
    }

    //predictions.show()

    if (needClusteringEvaluator) {
      // Evaluate clustering by computing Silhouette score
      val evaluator = new ClusteringEvaluator()
        .setFeaturesCol(DFStruct.FEATURE)
        .setMetricName("silhouette")

      val silhouette = evaluator.evaluate(predictions)
      println(s"Silhouette with squared euclidean distance = $silhouette")
    }

    predictions.drop(DFStruct.FEATURE)
  }
}

package com.tencent.angel.spark.ml.featurePreprocess.HashingTF

import com.tencent.angel.spark.ml.util.{DataFrameUtil, DataLoader, DataSaver, HDFSUtils}
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, Tokenizer}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by isakjiang on 2020-11-16
 */
object HashingTFLocal {

  //一篇文章中词语出现的最小词频，若为>=1的整数，表示词语出现的次数，
  // 若为[0,1)的Double类型值，表示词语出现数目与该篇文章词语总数的比值
  val MINTF = "minTF"
  //词语出现在不同文章中的最小数量，若为>=1的整数，表示词语出现在不同文章中的数量，
  // 若为[0,1)的Double类型值，表示词语出现在不同文章中的数量与总文章数的比值
  val MINDF = "minDF"
  val OUTPUT_TF = "outputTf"
  val OUTPUT_DIC = "outputDic"
  val DOC_COL = "docCol"

  def main(args: Array[String]) = {

    val mode = "local"
    val input = "spark-on-angel/mllib/src/main/scala/com/tencent/angel/spark/ml/featurePreprocess/MLDataSetInput/text_test.txt"
    val outputTf = "spark-on-angel/mllib/src/main/scala/com/tencent/angel/spark/ml/featurePreprocess/MLDataSetOutput/outputTf"
    val outputDic = "spark-on-angel/mllib/src/main/scala/com/tencent/angel/spark/ml/featurePreprocess/MLDataSetoutput/outputDic"
    val partitionNum = 1
    val sampleRate = 1
    val sep = " "
    //算法参数
    val minTF = 1
    val minDF = 1
    val textCol = 0
    //documentID所在列
    val docCol = -1


    val sc = start(mode)


    run(sc,
      input,
      outputTf,
      outputDic,
      partitionNum,
      sampleRate,
      minTF,
      minDF,
      textCol,
      docCol,
      sep)
  }

  def run(sc: SparkContext,
          input: String,
          outputTf: String,
          outputDic: String,
          partitionNum: Int,
          sampleRate: Double,
          minTF: Double,
          minDF: Double,
          textCol: Int,
          docCol: Int,
          sep: String) {
    //字段名定义
    val words = "words"
    val countTf = "countTf"
    val result = "result"

    val totalText = DataLoader.loadByLine(sc, input, partitionNum, sampleRate)
    val textColName = totalText.schema.fields(textCol).name

    //remove the empty str record
    val textFilterNull = DataFrameUtil.removeNullString(totalText, textColName)

    //切分句子，英文转化为小写
    val tokenizer = new Tokenizer().setInputCol(textColName).setOutputCol(words)
    val wordsData = tokenizer.transform(textFilterNull).drop(textColName)

    //计算tf值
    val countTF: CountVectorizerModel = new CountVectorizer()
      .setInputCol(words).setOutputCol(countTf)
      .setMinDF(minDF)
      .setMinTF(minTF)
      .fit(wordsData)

    //得到整个词典
    val voc = countTF.vocabulary
    val tfData = countTF.transform(wordsData).drop(words)


    //保存词典
    //在voc中提取符合minTF的word,one-based
    val voc_minTF = tfData.select(countTf).rdd.flatMap { case Row(x: SparseVector) => x.indices }
      .map(x => ((x + 1).toLong, 1))
    //wordsDic,one-based
    val wordsDic = voc.zipWithIndex.map { x =>
      ((x._2.toLong + 1), x._1)
    }
    val wordsDicRDD = sc.makeRDD(wordsDic)
    val dicResult = voc_minTF.distinct.leftOuterJoin(wordsDicRDD).map { case (index, (index2One, word)) =>
      Array(index.toString, word.getOrElse().toString)
    }
    DataSaver.save(dicResult, outputDic, sep)


    //UDF:tf转化为index:tf，one-based
    val tf2indexTfUDF = udf { (raw: SparseVector) =>
      (0 until raw.indices.size).map { i =>
        (raw.indices(i) + 1) + ":" + raw.values(i)
      }.mkString(" ")
    }

    val indextf = tfData.withColumn(countTf, tf2indexTfUDF(col(countTf)))

    //如果存在docId列，则每行整合成libsvm形式
    val docWithIndex2TfUDF = udf { (doc: Any, raw: String) =>
      val docId = if (doc == null) "" else doc
      docId + " " + raw
    }

    val result_tf = if (docCol < 0)
      indextf.withColumnRenamed(countTf, result)
    else {
      val docColName = textFilterNull.schema.fields(docCol).name
      indextf.withColumn(result, docWithIndex2TfUDF(col(docColName), col(countTf))).drop(countTf).drop(docColName)
    }
    val resultRdd = result_tf.select(result).rdd.map { case Row(record: String) => Array(record) }
    //仅保存HDFS路径
    HDFSUtils.save(resultRdd, outputTf, sep)
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

}

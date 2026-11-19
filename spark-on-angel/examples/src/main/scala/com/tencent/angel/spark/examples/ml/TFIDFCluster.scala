package com.tencent.angel.spark.examples.ml

import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.util.{DataFrameUtil, DataLoader, DataSaver, HDFSUtils}
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, IDF, Tokenizer}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by isakjiang on 2020-11-16
 */
object TFIDFCluster {

  //一篇文章中词语出现的最小词频，若为>=1的整数，表示词语出现的次数，
  // 若为[0,1)的Double类型值，表示词语出现数目与该篇文章词语总数的比值
  val MINTF = "minTF"
  //词语出现在不同文章中的最小数量，若为>=1的整数，表示词语出现在不同文章中的数量，
  //若为[0,1)的Double类型值，表示词语出现在不同文章中的数量与总文章数的比值
  val MINDF = "minDF"
  val OUTPUT_TFIDF = "outputTfIdf"
  val OUTPUT_DIC = "outputDic"
  val DOC_COL = "docCol"
  val IDF_MODEL_PATH = "idfModelPath"

  def main(args: Array[String]) {

    val params = ArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn")
    val input = params.getOrElse("input", null)
    val outputTfIdf = params.getOrElse("outputTfIdf", null)
    val outputDic = params.getOrElse("outputDic", null)
    val partitionNum = params.getOrElse("partitionNum", "10").toInt
    val sampleRate = params.getOrElse("sampleRate", "1.0").toDouble

    //算法参数
    val minTF = params.getOrElse(MINTF, "1").toDouble
    val minDF = params.getOrElse(MINDF, "1").toDouble
    val textCol = params.getOrElse("textCol", "0").toInt
    //documentID所在列
    val docCol = params.getOrElse(DOC_COL, "-1").toInt
    //the path to save idf model
    val idfModelPath = params.getOrElse("idfModelPath", null)

    val sep = params.getOrElse("sep", "space") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
    }


    val sc = start(mode)

    run(sc,
      input,
      outputTfIdf,
      outputDic,
      partitionNum,
      sampleRate,
      minTF,
      minDF,
      textCol,
      docCol,
      idfModelPath,
      sep)
  }

  /** TFIDF
   *
   * @param sc
   * @param input
   * @param outputTfIdf
   * @param outputDic
   * @param partitionNum
   * @param sampleRate
   * @param minTF
   * @param minDF
   * @param textCol
   * @param docCol
   */

  def run(sc: SparkContext,
          input: String,
          outputTfIdf: String,
          outputDic: String,
          partitionNum: Int,
          sampleRate: Double,
          minTF: Double,
          minDF: Double,
          textCol: Int,
          docCol: Int,
          idfModelPath: String,
          sep: String) {

    //字段名定义
    val words = "words"
    val countTf = "countTf"
    val tfidf = "tfidf"
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
    val featurizedData = countTF.transform(wordsData).drop(words)

    //idf
    val idf = new IDF().setInputCol(countTf).setOutputCol(tfidf)
    val idfModel = idf.fit(featurizedData)

    // save the idf model,which is a  vector
    if (idfModelPath != null) {
      val idfVector = idfModel.idf.toArray.map(_.toString)
      val idfVecRdd = sc.makeRDD(Array(idfVector))
      DataSaver.save(idfVecRdd, idfModelPath, sep)
    }

    //tf-idf值
    val tfidfData = idfModel.transform(featurizedData).drop(countTf)

    //在voc中提取符合minTF的word,one-based
    val voc_minTF = tfidfData.select(tfidf).rdd.flatMap { case Row(x: SparseVector) => x.indices }.map(x => ((x + 1).toLong, 1))
    //wordsDic,one-based
    val wordsDic = voc.zipWithIndex.map { x =>
      ((x._2.toLong + 1), x._1)
    }
    val wordsDicRDD = sc.makeRDD(wordsDic)
    val dicResult = voc_minTF.distinct.leftOuterJoin(wordsDicRDD).map { case (index, (index2One, word)) =>
      Array(index.toString, word.getOrElse().toString)
    }
    DataSaver.save(dicResult, outputDic, sep)


    //UDF:转化为index:tfidf
    val index2tfidfUDF = udf { (raw: SparseVector) =>
      (0 until raw.indices.size).map { i =>
        (raw.indices(i) + 1) + ":" + raw.values(i)
      }.mkString(" ")
    }

    val index2tfidf = tfidfData.withColumn(tfidf, index2tfidfUDF(col(tfidf)))

    //如果存在docId列，则每行整合成libsvm形式
    val docWithIndex2TfIdfUDF = udf { (doc: Any, raw: String) =>
      val docId = if (doc == null) "" else doc
      docId + " " + raw
    }

    val result_tfidf = if (docCol < 0)
      index2tfidf.withColumnRenamed(tfidf, result)
    else {
      val docColName = textFilterNull.schema.fields(docCol).name
      index2tfidf.withColumn(result, docWithIndex2TfIdfUDF(col(docColName), col(tfidf))).drop(tfidf).drop(docColName)
    }
    val resultRdd = result_tfidf.select(result).rdd.map { case Row(record: String) => Array(record) }
    //仅保存HDFS路径
    //    resultRdd.saveAsTextFile(outputTfIdf)
    HDFSUtils.save(resultRdd, outputTfIdf, sep)

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

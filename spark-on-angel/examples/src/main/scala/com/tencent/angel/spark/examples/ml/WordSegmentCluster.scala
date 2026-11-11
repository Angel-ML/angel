package com.tencent.angel.spark.examples.ml

import java.io.{BufferedReader, InputStreamReader}
import com.tencent.angel.spark.ml.util._
import org.ansj.library.UserDefineLibrary
import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.{SparkConf, SparkContext}
import org.nlpcn.commons.lang.tire.domain.{Forest, Value}
import org.nlpcn.commons.lang.tire.library.Library
import com.tencent.angel.spark.ml.core.{ArgsUtil => coreArgsUtil}
/**
 * Created by isakjiang on 2020/12/30.
  */
object WordSegmentCluster {
  //用户自定义的词典路径
  val USER_DEFINE_DIC = "userDefineDic"
  //分词词性开关，默认不输出词性
  val HAS_NATURE = "hasNature"
  val DEFAULT_DICT_PATH = "/dictionary/default.dic"
  val AMBIGUITY_DICT_PATH = "/dictionary/ambiguity.dic"
  val WORD_SEP = " "

  def main(args: Array[String]) {

    val params = coreArgsUtil.parse(args)

    val mode = params.getOrElse("mode", "yarn")
    val input = params.getOrElse("input", null)
    val textCol = params.getOrElse("textCol", "0").toInt
    val partitionNum = params.getOrElse(ParamKeys.PARTITION_NUM, "10").toInt
    val output = params.getOrElse(ParamKeys.OUTPUT, null)
    val userDicPath = params.getOrElse(USER_DEFINE_DIC, null)
    val hasNature = params.getOrElse(HAS_NATURE, "False").toBoolean


    //初始化用户自定义词典
    val udic = new Forest
    //初始化歧义词典
    val adic = new Forest

    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster(mode)
    val sc = new SparkContext(mode, this.getClass.getSimpleName, conf)
    sc.setLogLevel("WARN")

    //加载default.dic
    val userDictStream = this.getClass.getResourceAsStream(DEFAULT_DICT_PATH)
    val br = new BufferedReader(new InputStreamReader(userDictStream, "UTF-8"))
    var line = br.readLine()
    var defaultCount = 0
    while (line != null) {
      val line_split = line.split("\t")
      Library.insertWord(udic, new Value(line_split(0), line_split(1), line_split(2)))
      defaultCount += 1
      line = br.readLine()
    }
    br.close()
    println(s"Default dictionary size: $defaultCount")

    //加载ambiguity.dic
    val ambDictStream = this.getClass.getResourceAsStream(AMBIGUITY_DICT_PATH)
    val ambr = new BufferedReader(new InputStreamReader(ambDictStream, "UTF-8"))
    var ambline = ambr.readLine()
    while (ambline != null) {
      Library.insertWord(adic, new Value(ambline))
      ambline = ambr.readLine()
    }
    ambr.close()

    //加载用户自定义词典
    if (userDicPath != null) {
      val defaultPartitionNum = 10
      val userDictDF = DataLoader.loadByLine(sc, userDicPath, defaultPartitionNum, 1.0)

      //remove the empty str record
      val userDicColName = userDictDF.schema.fields(0).name
      val userDicFilterNull = DataFrameUtil.removeNullString(userDictDF, userDicColName)

      val userDic = userDicFilterNull.collect()
      userDic.foreach(word => {
        val line = if (word == null) "" else word(0).asInstanceOf[String]
        Library.insertWord(udic, new Value(line, "udf", "1000"))
      })
      println(s"User defined dictionary size: ${userDic.length}")
    }

    val uDicBC = sc.broadcast(udic)
    val aDicBC = sc.broadcast(adic)

    //开始分词
    val inputTable = DataLoader.loadByLine(sc, input, partitionNum, 1.0)
    val textColName = inputTable.schema.fields(textCol).name

    //remove the empty str record
    val textFilterNull = DataFrameUtil.removeNullString(inputTable, textColName)

    val segmentUDF = udf { (raw: Any) =>
      val line = if (raw == null) "" else raw.asInstanceOf[String]
      UserDefineLibrary.FOREST = uDicBC.value
      UserDefineLibrary.ambiguityForest = aDicBC.value
      //推荐分词方式
      val temp = ToAnalysis.parse(line)
      val word = for (i <- Range(0, temp.size())) yield {
        if (hasNature) temp.get(i) else temp.get(i).getName
      }
      word.mkString(WORD_SEP)
    }

    val outputDF = textFilterNull.withColumn("segWords", segmentUDF(col(textColName))).drop(textColName)

    println(s"Output text count: ${outputDF.count()}")
    DataSaver.save(outputDF, output,"")
  }

}

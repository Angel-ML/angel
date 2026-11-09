package com.tencent.angel.spark.ml.fpm.Apriori


import com.tencent.angel.spark.ml.fpm.Apriori.FrequentPatternMining._
import com.tencent.angel.spark.ml.fpm.Apriori.MineAssociateRuleBatch._
import com.tencent.angel.spark.ml.util.LogUtils
import org.apache.spark.rdd.RDD

class Apriori(private var delimiter: String,
              private var support: Double,
              private var smax: Double,
              private var minlen: Int,
              private var maxlen: Int,
              private var target: String,
              private var confidenct: Double,
              private var appearance: Array[String],
              private var sort: Int) extends Serializable {

  def this() = this(",", 0.1, 1, 1, 10, "rules", 0.8, Array("none", "0"), 0)

  def setDelimiter(delimiter: String): this.type = {
    this.delimiter = delimiter
    this
  }

  def setSupport(support: Double): this.type = {
    require(support > 0 && support <= 1, "the value of support should between 0 and 1," +
      " but it is " + support)
    this.support = support
    this
  }

  def setSmax(smax: Double): this.type = {
    require(smax > 0 && smax <= 1, "the value of smax should between 0 and 1," +
      " but it is " + smax)
    this.smax = smax
    this
  }

  def setMinlen(minlen: Int): this.type = {
    this.minlen = minlen
    this
  }

  def setMaxlen(maxlen: Int): this.type = {
    this.maxlen = maxlen
    this
  }

  def setConfidence(confidenct: Double): this.type = {
    require(confidenct > 0 && confidenct <= 1, "the value of confident should between 0 and 1," +
      " but it is " + confidenct)
    this.confidenct = confidenct
    this
  }

  def setAppearance(appearance: Array[String]): this.type = {
    LogUtils.logTime("the value of appearance: " + appearance.mkString(","))
    require(appearance.length % 2 == 0, "unsupport appearance: " + appearance.mkString(","))
    this.appearance = appearance
    this
  }

  def setSort(sort: Int): this.type = {
    //    1 asc, -1 desc, 0 no sort
    require(Array(1, -1, 0).contains(sort), "unsupport value of sort: " + sort)
    this.sort = sort
    this
  }

  def runSet(data: RDD[String]): RDD[(String, Double)] = {
    LogUtils.logTime("start to run apriori with following parameters: ")
    LogUtils.logTime("delimiter: " + delimiter)
    LogUtils.logTime("support:" + support)
    LogUtils.logTime("maxlen:" + maxlen)
    val fpm = apriori(data, delimiter, support, maxlen, sort)
    fpmFilter(fpm)
  }

  def runRule(data: RDD[String]): RDD[(String, Double, Double, Double)] = {
    LogUtils.logTime("start to run associatedRules with following parameters: ")
    LogUtils.logTime("delimiter: " + delimiter)
    LogUtils.logTime("support:" + support)
    LogUtils.logTime("confidence: " + confidenct)
    LogUtils.logTime("maxlen:" + maxlen)
    val rules = associatedRules(data, delimiter, support, confidenct, maxlen, sort)
    ruleFilter(rules)
  }

  def fpmFilter(data: RDD[(String, Double)]): RDD[(String, Double)] = {
    data.filter(record => {
      val set = record._1.split(delimiter)
      set.length >= minlen && set.length <= maxlen && record._2 >= support && record._2 <= smax
    })
  }

  def ruleFilter(data: RDD[(String, Double, Double, Double)]):
  RDD[(String, Double, Double, Double)] = {
    //    "rules", "support", "confidence", "lift"
    data.filter(record => {
      var lhs: Set[String] = null
      var rhs: Set[String] = null
      try {
        val set = record._1.split("=>")
        lhs = set(0).split(delimiter).toSet
        rhs = set(1).split(delimiter).toSet
      } catch {
        case e: ArrayIndexOutOfBoundsException => {
          LogUtils.logTime("ruleFilter error, rule is: " + record)
          e.printStackTrace()
        }
      }

      val sup = record._2
      val conf = record._3
      sup >= support && sup <= smax && conf >= confidenct && appearanceFilter(lhs, rhs)
    })
  }

  def appearanceFilter(lset: Set[String], rset: Set[String]): Boolean = {
    val size = (lset ++ rset).size
    val condition = appearance(0)
    val value = appearance(1)
    condition match {
      case "lhs" => size >= minlen && size <= maxlen && lset.contains(value)
      case "rhs" => size >= minlen && size <= maxlen && rset.contains(value)
      case "none" => size >= minlen && size <= maxlen
      case _ => LogUtils.logTime("unsupport appearance condition: " + condition); false
    }
  }
}

object Apriori {
  private val rules = "rules"
  private val fqtset = "frequent itemsets"
  private val maxfqtset = "maximally frequent itemsets"

  def apriori(
               data: RDD[String],
               delimiter: String,
               support: Double,
               smax: Double,
               minlen: Int,
               maxlen: Int,
               target: String,
               sort: Int): RDD[(String, Double)] = {
    require(target.toLowerCase() == fqtset)
    new Apriori()
      .setDelimiter(delimiter)
      .setSupport(support)
      .setSmax(smax)
      .setMinlen(minlen)
      .setMaxlen(maxlen)
      .setSort(sort)
      .runSet(data)
  }

  def apriori(
               data: RDD[String],
               delimiter: String,
               support: Double,
               smax: Double,
               minlen: Int,
               maxlen: Int,
               target: String,
               confidence: Double,
               appearance: Array[String],
               sort: Int): RDD[(String, Double, Double, Double)] = {
    require(target.toLowerCase() == rules)
    new Apriori()
      .setDelimiter(delimiter)
      .setSupport(support)
      .setSmax(smax)
      .setMinlen(minlen)
      .setMaxlen(maxlen)
      .setConfidence(confidence)
      .setAppearance(appearance)
      .setSort(sort)
      .runRule(data)
  }

}
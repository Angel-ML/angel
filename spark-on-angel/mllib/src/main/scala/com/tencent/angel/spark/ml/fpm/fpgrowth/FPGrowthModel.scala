package com.tencent.angel.spark.ml.fpm.fpgrowth

import org.apache.spark.SparkContext
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{DoubleParam, ParamMap, ParamValidators, Params}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}

class FPGrowthModel(val itemFreqSet: DataFrame,
                    val itemSupport: Map[Int, Double],
                    val numTrainingRecords: Long,
                    val idToItem: Option[Array[String]] = None
                    ) extends Transformer {
  override val uid: String = Identifiable.randomUID("FPGrowthModel")
  
  final val minConfidence: DoubleParam = new DoubleParam(this, "minConfidence", "minConfidence", ParamValidators.inRange(0.0, 1.0))
  final def setMinConfidence(conf: Double): this.type = set(minConfidence, conf)
  setDefault(minConfidence, 0d)
  
  var _cachedConfidence: Double = Double.NaN
  var _cachedRules: DataFrame = _
  
  def generateRules(): DataFrame = {
    if ($(minConfidence) == _cachedConfidence) {
      _cachedRules
    }
    else {
      _cachedRules = this.detectRules()
      _cachedConfidence = $(minConfidence)
      _cachedRules
    }
  }
  
  private def detectRules2(): DataFrame = {
    println(s"start detecting rules...")
    val itemsSet = this.itemFreqSet.select("items", "freq").rdd
      .filter(row => !row.anyNull)
      .map(row => (row.getSeq[Int](0), row.getLong(1)))
    
    println(s"#items ${itemsSet.count()}")
  
    val candidates = itemsSet.flatMap {
      case (items, freq) =>
        items.flatMap{ item =>
          val antecedent = items.filter(_ != item)
          if (antecedent.isEmpty) {
            None
          }
          else {
            Iterator.single((antecedent, (item, freq)))
          }
        }
    }
    
    val rules = candidates.join(itemsSet).map {
      case (antecedent, ((consequent, unionFreq), anteFreq)) =>
        new Rule(antecedent, consequent, unionFreq, anteFreq, this.itemSupport.get(consequent))
    }.filter(_.confidence >= $(minConfidence))
    
    println(s"#rules: ${rules.count()}")
    val rows = rules.flatMap{ rule =>
      Iterator.single(Row(rule.antecedent.mkString(" "),
        rule.consequent, rule.confidence,
        rule.lift.orNull,
        rule.freqUnion / numTrainingRecords))
    }
    
    val schema = StructType(Seq(
      StructField("antecedent", StringType, nullable = false),
      StructField("consequent", IntegerType, nullable = false),
      StructField("confidence", DoubleType, nullable = false),
      StructField("lift", DoubleType, nullable = true),
      StructField("support", DoubleType, nullable = false)))
    
    this.itemFreqSet.sparkSession.createDataFrame(rows, schema)
  }
  
  private def detectRules(): DataFrame = {
    println(s"start detecting rules...")
    val itemsSet = this.itemFreqSet.select("items", "freq").rdd
      .filter(row => !row.anyNull)
      .map(row => (row.getSeq[Int](0), row.getLong(1)))
    
    println(s"#items ${itemsSet.count()}")
    
    val candidates = itemsSet.flatMap {
      case (items, freq) =>
        items.flatMap{ item =>
          val antecedent = items.filter(_ != item)
          if (antecedent.isEmpty) {
            None
          }
          else {
            Iterator.single((antecedent, (item, freq)))
          }
        }
    }
    
    val rules = candidates.join(itemsSet).map {
      case (antecedent, ((consequent, unionFreq), anteFreq)) =>
        new Rule(antecedent, consequent, unionFreq, anteFreq, this.itemSupport.get(consequent))
    }.filter(_.confidence >= $(minConfidence))
    
    println(s"#rules: ${rules.count()}")
    
    val rows = idToItem match {
      case Some(idToItem) =>
        val sc = SparkContext.getOrCreate()
        val brItemMap = sc.broadcast(idToItem)
        rules.mapPartitions{ iter =>
          val itemMap = brItemMap.value
          iter.flatMap{ rule =>
            Iterator.single(Row(rule.antecedent.map(itemMap(_)).mkString(" "),
              itemMap(rule.consequent), rule.confidence,
              rule.lift.orNull,
              rule.freqUnion / numTrainingRecords))
          }
        }
      case None =>
        rules.flatMap{ rule =>
          Iterator.single(Row(rule.antecedent.mkString(" "),
            rule.consequent.toString, rule.confidence,
            rule.lift.orNull,
            rule.freqUnion / numTrainingRecords))
        }
    }
    
    val schema = StructType(Seq(
      StructField("antecedent", StringType, nullable = false),
      StructField("consequent", StringType, nullable = false),
      StructField("confidence", DoubleType, nullable = false),
      StructField("lift", DoubleType, nullable = true),
      StructField("support", DoubleType, nullable = false)))
    
    this.itemFreqSet.sparkSession.createDataFrame(rows, schema)
  }
  
  
  def generateFreqItems2(): DataFrame = {
    val itemsSetRDD = this.itemFreqSet.select("items", "freq").rdd
      .filter(row => !row.anyNull)
      .map(row => Row(row.getSeq[Int](0).mkString(" "), row.getLong(1).toDouble / numTrainingRecords))
    
    val schema = StructType(Seq(
      //StructField("items", ArrayType(IntegerType), nullable = false),
      StructField("items", StringType, nullable = false),
      StructField("support", DoubleType, nullable = false)
    ))
    
    this.itemFreqSet.sparkSession.createDataFrame(itemsSetRDD, schema)
  }
  
  def generateFreqItems(): DataFrame = {
    val itemsSetRDD = this.itemFreqSet.select("items", "freq").rdd
      .filter(row => !row.anyNull)
    
    val reformed =
      idToItem match {
        case Some(idToItem) =>
          val sc = SparkContext.getOrCreate()
          val brItemMap = sc.broadcast(idToItem)
    
          itemsSetRDD.mapPartitions{ iter =>
            val itemMap = brItemMap.value
            iter.map{ row =>
              Row(row.getSeq[Int](0).map(itemMap(_)).mkString(" "), row.getLong(1).toDouble / numTrainingRecords)
            }
          }
        case None =>
          itemsSetRDD.map(row => Row(row.getSeq[Int](0).mkString(" "), row.getLong(1).toDouble / numTrainingRecords))
      }
    
    val schema = StructType(Seq(
      //StructField("items", ArrayType(IntegerType), nullable = false),
      StructField("items", StringType, nullable = false),
      StructField("support", DoubleType, nullable = false)
    ))
    
    this.itemFreqSet.sparkSession.createDataFrame(reformed, schema)
  }
  
  override def transform(dataset: Dataset[_]): DataFrame = ???
  
  override def transformSchema(schema: StructType): StructType = ???
  
  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)
  
}

private[fpm] class Rule(val antecedent: Seq[Int],
                        val consequent: Int,
                        val freqUnion: Double,
                        val freqAntecedent: Double,
                        val consequentSupport: Option[Double]) extends Serializable {
  
  def confidence: Double = freqUnion / freqAntecedent
  
  def lift: Option[Double] = consequentSupport.map(consSupp => confidence / consSupp)
}

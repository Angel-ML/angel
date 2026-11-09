package org.apache.spark.ml.association


import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.shared.HasInputCol
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.{Identifiable, SchemaUtils}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import com.tencent.angel.spark.ml.fpm.Apriori.{Apriori => MLLIBApriori}
import com.tencent.angel.spark.ml.params.HasOutputCols
import org.apache.spark.sql.{DataFrame, Dataset, Row}


class Apriori(override val uid: String) extends Transformer with AprioriParam {

  setDefault(
    delimiter -> ","
  )

  def this() = this(Identifiable.randomUID("apriori"))

  override def copy(extra: ParamMap): Apriori = defaultCopy(extra)

  /** @group setParam */
  def setMinSupport(value: Double): this.type = set(minSupport, value)

  /** @group setParam */
  def setDelimiter(value: String): this.type = set(delimiter, value)

  /** @group setParam */
  def setMaxLength(value: Int): this.type = set(maxLength, value)

  /** @group setParam */
  def setMinLength(value: Int): this.type = set(minLength, value)

  /** @group setParam */
  def setMaxSupport(value: Double): this.type = set(maxSupport, value)

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCols(value: Array[String]): this.type = set(outputCols, value)

  /**
    * Transforms the input dataset.
    */
  override def transform(dataset: Dataset[_]): DataFrame = {
    val sqlContext = dataset.sqlContext

    val outputFields = transformSchema(dataset.schema, logging = true)

    val rdd = dataset.select(col($(inputCol))).rdd
      .map { case Row(arr: Seq[String]) => arr.mkString($(delimiter))}

    val count = rdd.count()

    val algo = new MLLIBApriori()
      .setSupport($(minSupport))
      .setDelimiter($(delimiter))
      .setMaxlen($(maxLength))
      .setMinlen($(minLength))
      .setSmax($(maxSupport))

    val freqItemsets = algo.runSet(rdd).map(t => (t._1, (t._2 * count).toLong))

    val df = sqlContext.createDataFrame(
      freqItemsets.map(x => Row(x._1.split($(delimiter)), x._2)), outputFields)
    df
  }

  /**
    * :: DeveloperApi ::
    *
    * Derives the output schema from the input schema.
    */
  override def transformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(
      schema,
      $(inputCol),
      DataTypes.createArrayType(DataTypes.StringType))

    val outputColNames = $(outputCols)

    val outputFields = StructType(Array(
      StructField(
        outputColNames(0),
        DataTypes.createArrayType(DataTypes.StringType),
        nullable = false),
      StructField(
        outputColNames(1),
        DataTypes.LongType,
        nullable = false)))
    outputFields
  }
}

trait AprioriParam extends Params with HasInputCol with HasOutputCols {
  /**
    * Set the minimal support level of the frequent pattern. Must be > 0 and < 1. Default: 0.5.
    * @group param
    */
  final val minSupport = new DoubleParam(
    this,
    "minSupport",
    "the minimal support level of the frequent pattern",
    (x: Double) => x >= 0 && x <= 1)

  def getMinSupport: Double = $(minSupport)

  final val delimiter = new Param[String](
    this,
    "delimiter",
    "delimiter")

  def getDelimiter: String = $(delimiter)

  /**
    * Set the maximum length of frequent itemsets. Default: 100000.
    * @group param
    */
  final val maxLength = new IntParam(
    this,
    "maxPatternLength",
    "the maximum length of frequent itemsets",
    (x: Int) => x >= 0
  )

  def getMaxLength: Int = $(maxLength)

  /**
    * Set the minimum length of frequent itemsets. Default: 0.
    * @group param
    */
  final val minLength = new IntParam(
    this,
    "minPatternLength",
    "the minimum length of frequent itemsets",
    (x: Int) => x >= 0
  )

  def getMinLength: Int = $(minLength)

  /**
    * Set the maximum value of support. Default: 1.
    * @group param
    */
  final val maxSupport = new DoubleParam(
    this,
    "maxSupport",
    "the maximum value of support",
    (x: Double) => x >= 0 && x <= 1
  )

  def getMaxSupport: Double = $(maxSupport)
}

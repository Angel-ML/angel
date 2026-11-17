package org.apache.spark.ml.feature


import com.tencent.angel.spark.ml.util.{DefaultAngelParamsReader, DefaultAngelParamsWriter}
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkException
import org.apache.spark.annotation.Since
import org.apache.spark.ml.attribute.NominalAttribute
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.sql.catalyst.expressions.{If, Literal}
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.collection.OpenHashMap

trait AngelStringIndexerBase extends Params {

  /**
   * Param for input column name.
   * @group param
   */
  final val inputCol: Param[String] = new Param[String](this, "inputCol", "input column name")

  /** @group getParam */
  final def getInputCol: String = $(inputCol)

  /**
   * Param for output column name.
   * @group param
   */
  final val outputCol: Param[String] = new Param[String](this, "outputCol", "output column name")

  setDefault(outputCol, uid + "__output")

  /** @group getParam */
  final def getOutputCol: String = $(outputCol)

  /**
   * Param for input column names.
   * @group param
   */
  final val inputCols: StringArrayParam = new StringArrayParam(this, "inputCols", "input column names")

  /** @group getParam */
  final def getInputCols: Array[String] = $(inputCols)

  /**
   * Param for output column names.
   * @group param
   */
  final val outputCols: StringArrayParam = new StringArrayParam(this, "outputCols", "output column names")

  /** @group getParam */
  final def getOutputCols: Array[String] = $(outputCols)

  /** @group getParam */
  final def getHandleInvalid: String = $(handleInvalid)

  @Since("1.6.0")
  final val handleInvalid: Param[String] = new Param[String](this, "handleInvalid",
    "How to handle invalid data (unseen labels or NULL values). " +
      "Options are 'skip' (filter out rows with invalid data), error (throw an error), " +
      "or 'keep' (put invalid data in a special additional bucket, at index numLabels).",
    ParamValidators.inArray(AngelStringIndexer.supportedHandleInvalids))

  setDefault(handleInvalid, AngelStringIndexer.ERROR_INVALID)

  final val stringOrderType: Param[String] = new Param(this, "stringOrderType",
    "How to order labels of string column. " +
      "The first label after ordering is assigned an index of 0. " +
      s"Supported options: ${AngelStringIndexer.supportedStringOrderType.mkString(", ")}.",
    ParamValidators.inArray(AngelStringIndexer.supportedStringOrderType))

  /** @group getParam */
  def getStringOrderType: String = $(stringOrderType)

  /** Returns the input and output column names corresponding in pair. */
  def getInOutCols: (Array[String], Array[String]) = {
    //    ParamValidators.checkSingleVsMultiColumnParams(this, Seq(outputCol), Seq(outputCols))

    if (isSet(inputCol)) {
      (Array($(inputCol)), Array($(outputCol)))
    } else {
      require($(inputCols).length == $(outputCols).length,
        "The number of input columns does not match output columns")
      ($(inputCols), $(outputCols))
    }
  }

  def validateAndTransformField(schema: StructType,
                                inputColName: String,
                                outputColName: String): StructField = {
    val inputDataType = schema(inputColName).dataType
    require(inputDataType == StringType || inputDataType.isInstanceOf[NumericType],
      s"The input column $inputColName must be either string type or numeric type, " +
        s"but got $inputDataType.")
    require(schema.fields.forall(_.name != outputColName),
      s"Output column $outputColName already exists.")
    NominalAttribute.defaultAttr.withName($(outputCol)).toStructField()
  }

  protected def validateAndTransformSchema(schema: StructType,
                                           skipNonExistsCol: Boolean = false): StructType = {
    val (inputColNames, outputColNames) = getInOutCols

    require(outputColNames.distinct.length == outputColNames.length,
      s"Output columns should not be duplicate.")

    val outputFields = inputColNames.zip(outputColNames).flatMap {
      case (inputColName, outputColName) =>
        schema.fieldNames.contains(inputColName) match {
          case true => Some(validateAndTransformField(schema, inputColName, outputColName))
          case false if skipNonExistsCol => None
          case _ => throw new SparkException(s"Input column $inputColName does not exist.")
        }
    }
    StructType(schema.fields ++ outputFields)
  }
}

class AngelStringIndexer(override val uid: String) extends Estimator[AngelStringIndexerModel]
  with AngelStringIndexerBase with DefaultParamsWritable {
  def this() = this(Identifiable.randomUID("strIdx"))

  def setHandleInvalid(value: String): this.type = set(handleInvalid, value)

  def setStringOrderType(value: String): this.type = set(stringOrderType, value)
  setDefault(stringOrderType, AngelStringIndexer.frequencyDesc)

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  def setInputCols(value: Array[String]): this.type = set(inputCols, value)

  def setOutputCols(value: Array[String]): this.type = set(outputCols, value)

  private def countByValue(dataset: Dataset[_],
                           inputCols: Array[String]): Array[OpenHashMap[String, Long]] = {

    val aggregator = new JarvisStringIndexerAggregator(inputCols.length)
    implicit val encoder = Encoders.kryo[Array[OpenHashMap[String, Long]]]

    val selectedCols = inputCols.map { colName =>
      val col = dataset.col(colName)
      if (col.expr.dataType == StringType) {
        col
      } else {
        new Column(If(col.isNaN.expr, Literal(null), col.expr)).cast(StringType)
      }
    }

    dataset.select(selectedCols: _*)
      .toDF
      .groupBy().agg(aggregator.toColumn)
      .as[Array[OpenHashMap[String, Long]]]
      .collect()(0)
  }

  override def fit(dataset: Dataset[_]): AngelStringIndexerModel = {
    transformSchema(dataset.schema, logging = true)

    val (inputCols, _) = getInOutCols

    // If input dataset is not originally cached, we need to unpersist it
    // once we persist it later.
    val needUnpersist = dataset.storageLevel == StorageLevel.NONE

    // In case of equal frequency when frequencyDesc/Asc, the strings are further sorted
    // alphabetically.
    val labelsArray = $(stringOrderType) match {
      case AngelStringIndexer.frequencyDesc =>
        val sortFunc = AngelStringIndexer.getSortFunc(ascending = false)
        countByValue(dataset, inputCols).map { counts =>
          counts.toSeq.sortWith(sortFunc).map(_._1).toArray
        }
      case AngelStringIndexer.frequencyAsc =>
        val sortFunc = AngelStringIndexer.getSortFunc(ascending = true)
        countByValue(dataset, inputCols).map { counts =>
          counts.toSeq.sortWith(sortFunc).map(_._1).toArray
        }
      case AngelStringIndexer.alphabetDesc =>
        import dataset.sparkSession.implicits._
        dataset.persist()
        val labels = inputCols.map { inputCol =>
          dataset.select(inputCol).na.drop().distinct().sort(dataset(s"$inputCol").desc)
            .as[String].collect()
        }
        if (needUnpersist) {
          dataset.unpersist()
        }
        labels
      case AngelStringIndexer.alphabetAsc =>
        import dataset.sparkSession.implicits._
        dataset.persist()
        val labels = inputCols.map { inputCol =>
          dataset.select(inputCol).na.drop().distinct().sort(dataset(s"$inputCol").asc)
            .as[String].collect()
        }
        if (needUnpersist) {
          dataset.unpersist()
        }
        labels
    }
    copyValues(new AngelStringIndexerModel(uid, labelsArray).setParent(this))
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override def copy(extra: ParamMap): AngelStringIndexer = defaultCopy(extra)
}

object AngelStringIndexer extends DefaultParamsReadable[AngelStringIndexer] {
  val SKIP_INVALID: String = "skip"
  val ERROR_INVALID: String = "error"
  val KEEP_INVALID: String = "keep"
  val supportedHandleInvalids: Array[String] =
    Array(SKIP_INVALID, ERROR_INVALID, KEEP_INVALID)
  val frequencyDesc: String = "frequencyDesc"
  val frequencyAsc: String = "frequencyAsc"
  val alphabetDesc: String = "alphabetDesc"
  val alphabetAsc: String = "alphabetAsc"
  val supportedStringOrderType: Array[String] =
    Array(frequencyDesc, frequencyAsc, alphabetDesc, alphabetAsc)

  override def load(path: String): AngelStringIndexer = super.load(path)

  def getSortFunc(ascending: Boolean): ((String, Long), (String, Long)) => Boolean = {
    if (ascending) {
      case ((strA: String, freqA: Long), (strB: String, freqB: Long)) =>
        if (freqA == freqB) {
          strA < strB
        } else {
          freqA < freqB
        }
    } else {
      case ((strA: String, freqA: Long), (strB: String, freqB: Long)) =>
        if (freqA == freqB) {
          strA < strB
        } else {
          freqA > freqB
        }
    }
  }
}

class AngelStringIndexerModel(override val uid: String, val labelsArray: Array[Array[String]])
  extends Model[AngelStringIndexerModel] with AngelStringIndexerBase with MLWritable {
  import AngelStringIndexerModel._

  def this(uid: String, labels: Array[String]) = this(uid, Array(labels))

  def this(labels: Array[String]) = this(Identifiable.randomUID("strIdx"), Array(labels))

  def this(labelsArray: Array[Array[String]]) = this(Identifiable.randomUID("strIdx"), labelsArray)

  def labels: Array[String] = {
    require(labelsArray.length == 1, "This StringIndexerModel is fit on multiple columns. " +
      "Call `labelsArray` instead.")
    labelsArray(0)
  }

  private val labelsToIndexArray: Array[OpenHashMap[String, Double]] = {
    for (labels <- labelsArray) yield {
      val n = labels.length
      val map = new OpenHashMap[String, Double](n)
      labels.zipWithIndex.foreach { case (label, idx) =>
        map.update(label, idx)
      }
      map
    }
  }

  def setHandleInvalid(value: String): this.type = set(handleInvalid, value)

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  def setInputCols(value: Array[String]): this.type = set(inputCols, value)

  def setOutputCols(value: Array[String]): this.type = set(outputCols, value)

  // This filters out any null values and also the input labels which are not in
  // the dataset used for fitting.
  private def filterInvalidData(dataset: Dataset[_], inputColNames: Seq[String]): Dataset[_] = {
    val conditions: Seq[Column] = inputColNames.indices.map { i =>
      val inputColName = inputColNames(i)
      val labelToIndex = labelsToIndexArray(i)
      // We have this additional lookup at `labelToIndex` when `handleInvalid` is set to
      // `StringIndexer.SKIP_INVALID`. Another idea is to do this lookup natively by SQL
      // expression, however, lookup for a key in a map is not efficient in SparkSQL now.
      // See `ElementAt` and `GetMapValue` expressions. If SQL's map lookup is improved,
      // we can consider to change this.
      val filter = udf { label: String =>
        labelToIndex.contains(label)
      }
      filter(dataset(inputColName))
    }

    dataset.na.drop(inputColNames.filter(dataset.schema.fieldNames.contains(_)))
      .where(conditions.reduce(_ and _))
  }

  private def getIndexer(labels: Seq[String], labelToIndex: OpenHashMap[String, Double]) = {
    val keepInvalid = getHandleInvalid == AngelStringIndexer.KEEP_INVALID

    udf { label: String =>
      if (label == null) {
        if (keepInvalid) {
          labels.length
        } else {
          throw new SparkException("StringIndexer encountered NULL value. To handle or skip " +
            "NULLS, try setting StringIndexer.handleInvalid.")
        }
      } else {
        if (labelToIndex.contains(label)) {
          labelToIndex(label)
        } else if (keepInvalid) {
          labels.length
        } else {
          throw new SparkException(s"Unseen label: $label. To handle unseen labels, " +
            s"set Param handleInvalid to ${StringIndexer.KEEP_INVALID}.")
        }
      }
    }  // .asNondeterministic()
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)

    val (inputColNames, outputColNames) = getInOutCols
    val outputColumns = new Array[Column](outputColNames.length)

    // Skips invalid rows if `handleInvalid` is set to `StringIndexer.SKIP_INVALID`.
    var filteredDataset = if (getHandleInvalid == StringIndexer.SKIP_INVALID) {
      filterInvalidData(dataset, inputColNames)
    } else {
      dataset
    }

    for (i <- outputColNames.indices) {
      val inputColName = inputColNames(i)
      val outputColName = outputColNames(i)
      val labelToIndex = labelsToIndexArray(i)
      val labels = labelsArray(i)

      if (!dataset.schema.fieldNames.contains(inputColName)) {
        logWarning(s"Input column $inputColName does not exist during transformation. " +
          "Skip StringIndexerModel for this column.")
        outputColNames(i) = null
      } else {
        val filteredLabels = getHandleInvalid match {
          case StringIndexer.KEEP_INVALID => labels :+ "__unknown"
          case _ => labels
        }
        val metadata = NominalAttribute.defaultAttr
          .withName(outputColName)
          .withValues(filteredLabels)
          .toMetadata()

        val indexer = getIndexer(labels, labelToIndex)

        outputColumns(i) = indexer(dataset(inputColName).cast(StringType))
          .as(outputColName, metadata)
      }
    }

    val filteredOutputColNames = outputColNames.filter(_ != null)
    val filteredOutputColumns = outputColumns.filter(_ != null)

    require(filteredOutputColNames.length == filteredOutputColumns.length)
    if (filteredOutputColNames.length > 0) {
      for (i <- filteredOutputColNames.indices) {
        filteredDataset = filteredDataset.withColumn(filteredOutputColNames(i), filteredOutputColumns(i))
      }
      filteredDataset.toDF()
    } else {
      filteredDataset.toDF()
    }
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema, skipNonExistsCol = true)
  }

  override def copy(extra: ParamMap): AngelStringIndexerModel = {
    val copied = new AngelStringIndexerModel(uid, labelsArray)
    copyValues(copied, extra).setParent(parent)
  }

  override def write: JarvisStringIndexModelWriter = new JarvisStringIndexModelWriter(this)
}

object AngelStringIndexerModel extends MLReadable[AngelStringIndexerModel] {
  class JarvisStringIndexModelWriter(instance: AngelStringIndexerModel) extends MLWriter {

    private case class Data(labelsArray: Array[Array[String]])

    override protected def saveImpl(path: String): Unit = {
      DefaultAngelParamsWriter.saveMetadata(instance, path, sc)
      val data = Data(instance.labelsArray)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class JarvisStringIndexerModelReader extends MLReader[AngelStringIndexerModel] {

    private val className = classOf[AngelStringIndexerModel].getName

    override def load(path: String): AngelStringIndexerModel = {
      val metadata = DefaultAngelParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString

      // We support loading old `StringIndexerModel` saved by previous Spark versions.
      // Previous model has `labels`, but new model has `labelsArray`.
      val data = sparkSession.read.parquet(dataPath)
        .select("labelsArray")
        .head()
      val labelsArray = data.getAs[Seq[Seq[String]]](0).map(_.toArray).toArray

      val model = new AngelStringIndexerModel(metadata.uid, labelsArray)
      DefaultAngelParamsReader.getAndSetParams(model, metadata)
      model
    }
  }

  override def read: MLReader[AngelStringIndexerModel] = new JarvisStringIndexerModelReader

  override def load(path: String): AngelStringIndexerModel = super.load(path)
}

private class JarvisStringIndexerAggregator(numColumns: Int)
  extends Aggregator[Row, Array[OpenHashMap[String, Long]], Array[OpenHashMap[String, Long]]] {

  override def zero: Array[OpenHashMap[String, Long]] =
    Array.fill(numColumns)(new OpenHashMap[String, Long]())

  def reduce(array: Array[OpenHashMap[String, Long]],
             row: Row): Array[OpenHashMap[String, Long]] = {
    for (i <- 0 until numColumns) {
      val stringValue = row.getString(i)
      // We don't count for null values.
      if (stringValue != null) {
        array(i).changeValue(stringValue, 1L, _ + 1)
      }
    }
    array
  }

  def merge(array1: Array[OpenHashMap[String, Long]],
            array2: Array[OpenHashMap[String, Long]]): Array[OpenHashMap[String, Long]] = {
    for (i <- 0 until numColumns) {
      array2(i).foreach { case (key: String, count: Long) =>
        array1(i).changeValue(key, count, _ + count)
      }
    }
    array1
  }

  def finish(array: Array[OpenHashMap[String, Long]]): Array[OpenHashMap[String, Long]] = array

  override def bufferEncoder: Encoder[Array[OpenHashMap[String, Long]]] = {
    Encoders.kryo[Array[OpenHashMap[String, Long]]]
  }

  override def outputEncoder: Encoder[Array[OpenHashMap[String, Long]]] = {
    Encoders.kryo[Array[OpenHashMap[String, Long]]]
  }
}



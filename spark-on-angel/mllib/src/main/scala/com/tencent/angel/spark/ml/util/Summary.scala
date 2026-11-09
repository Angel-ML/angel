package com.tencent.angel.spark.ml.util

import java.io.PrintWriter

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.json4s.JValue
import org.json4s.JsonAST.{JDouble, JArray, JField}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import scala.collection.immutable.HashMap
import scala.collection.mutable.ArrayBuffer
import java.net.URI


/**
 * Created by kevinzwyou on 16-10-11.
 */

object Summary{
  def MetricFileName = "/metrics.json"
  def MetaFileName = "/meta.info"
  def EvaluateFileName = "/evaluation.info"
}

trait Summary extends Serializable {

  protected def flush(fileName: String, context: String): Unit = {

    // TODO 1: 短时间多次append, 最好等待5s, 避免短时间多次flush磁盘
    // TODO 2: fileName文件是否存在, 或者是否和已有目录重名
    val hdfsConf = new Configuration()
    val fs = FileSystem.get(new URI(fileName), hdfsConf)
    val outputFile = new Path(fileName)
    val writer = new PrintWriter(fs.create(outputFile))
    writer.println(context)
    writer.close()
  }
}

class MetricsSummary private (fileName: String) extends Summary {

  class ModelMetrics(tag: String) extends Serializable {
    private var history = new ArrayBuffer[(Double, Double)]()
    private val HistroyMaxLength = 1e5.toInt

    def append(x: Int, y: Double): this.type = {
      if (history.length < HistroyMaxLength) {
        history += (x.toDouble -> y)
      } else {
        println(s"[WARN] Model metric history is bigger than ${history.length}, append failed")

      }
      this
    }

    def append(hist: Array[(Double, Double)]): this.type = {
      if (history.length + hist.length < HistroyMaxLength) {
        history ++= hist
      } else {
        println(s"[WARN] Model metric history is bigger than ${history.length}, append failed")
      }
      this
    }

    def toJson(): JValue = {
      history.map {
        case (x, y) => JArray(List(JDouble(x), JDouble(y)))
      }
    }
  }

  private var summary = new HashMap[String, ModelMetrics]()

  def logMetrics(tag: String, x: Int, y: Double): this.type = {
    val metric = summary.getOrElse(tag, new ModelMetrics(tag))
    summary += (tag -> metric.append(x, y))
    flush()
    this
  }

  def logMetrics(tag: String, list: Array[(Double, Double)]): this.type = {
    val metric = summary.getOrElse(tag, new ModelMetrics(tag))
    summary += (tag -> metric.append(list))
    flush()
    this
  }


  /**
    * json format:
    * {
    *   figure1: {
    *     tag1: [ [0, 0.0],[1, 0.1],[2, 0.3],[3, 0.7] ],
    *     tag2: [ [0, 0], [1, 1],[2, 3],[3, 7] ]
    *   }
    * }
    *
    */

  private def flush() {
    val json = JField("figure1",
      summary.map {
        case (tag, metric) => JField(tag, metric.toJson())
      }
    )
    val jsonString = pretty(render(json))

    super.flush(fileName, jsonString)
  }
}


object MetricsSummary {
  def apply(modelPath: String):  MetricsSummary = {
    new  MetricsSummary(modelPath + Summary.MetricFileName)
  }
}

class MetaSummary private (fileName: String) extends Summary {
  private var metaData = new HashMap[String, Number]()

  def logMeta(metaPairs: Array[(String, Number)]): this.type = {
    metaData ++= metaPairs
    flush()
    this
  }

  def logMeta(key: String, value: Number): this.type = {
    metaData += (key -> value)
    flush()
    this
  }

  private def flush() {
    /**
     * json format
     * {
     *  "train instance num" : "1000",
     *  "feature num" : "100"
     * }
     */
    val json = metaData.map {
      case (key, value) => JField(key, value.toString())
    }.toMap

    val jsonString = pretty(render(json))
    super.flush(fileName, jsonString)
  }
}


object MetaSummary {
  def initTrainMeta(modelPath: String): MetaSummary = {
    new MetaSummary(modelPath + Summary.MetaFileName)
  }

  def initEvaluateMeta(modelPath: String): MetaSummary = {
    new MetaSummary(modelPath + Summary.EvaluateFileName)
  }
}



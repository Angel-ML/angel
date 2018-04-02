package com.tencent.angel.serving.laucher

import java.io.{BufferedWriter, File, FileWriter}
import java.net.InetSocketAddress

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.ml.conf.MLConf
import com.tencent.angel.ml.math.TVector
import com.tencent.angel.serving.client.{DistributedModel, ServingClient}
import com.tencent.angel.serving.{PredictData, Util}
import com.tencent.angel.utils.ConfUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat

object ServingPredictClient {

  private def getConf(args: Array[String]): Configuration = {
    val conf: Configuration = ConfUtils.initConf(args)

    conf.setBoolean(AngelConf.ANGEL_AM_USE_DUMMY_DATASPLITER, true)
    conf.setBoolean(AngelConf.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST, true)
    conf.set(AngelConf.ANGEL_INPUTFORMAT_CLASS, classOf[CombineTextInputFormat].getName)

    conf
  }

  private def createServingClient(conf: Configuration): ServingClient = {
    val masterIP = conf.get(AngelConf.ANGEL_SERVING_MASTER_IP)
    val masterPort = conf.getInt(AngelConf.ANGEL_SERVING_MASTER_PORT, -1)
    if (masterPort == -1) {
      println("Invalide port, please check!")
      System.exit(0)
    }

    val masterAddr = new InetSocketAddress(masterIP, masterPort)
    ServingClient.create(masterAddr, conf)
  }

  private def retrieveModel(servingClient: ServingClient, conf: Configuration): Option[DistributedModel] = {
    val modelpath = conf.get(AngelConf.ANGEL_LOAD_MODEL_PATH)
    val replica = conf.getInt(AngelConf.ANGEL_SERVING_SHARDING_NUM, 3)
    val concurrent = conf.getInt(AngelConf.ANGEL_SERVING_SHARDING_CONCURRENT_CAPACITY, 3)
    val modelName = conf.get(AngelConf.ANGEL_SERVING_MODEL_NAME)

    servingClient.loadModelLocal(modelName, modelpath, replica, concurrent)
    var wait: Long = 0
    val timeout: Long = conf.getLong(AngelConf.ANGEL_SERVING_MODEL_LOAD_TIMEOUT_MINUTE, 5) * 60 * 1000
    val interval: Long = conf.getLong(AngelConf.ANGEL_SERVING_MODEL_LOAD_CHECK_INTEVAL_SECOND, 30) * 1000
    while (!servingClient.isServable(modelName) && wait < timeout) {
      wait += interval
      println("Server is not servable, pls wait ...")
      Thread.sleep(interval)
    }
    if (!servingClient.isServable(modelName)) {
      println("Server is still not servable after many retry, there may be a problem!")
      System.exit(0)
    }
    println("load model finished!")

    servingClient.getModel(modelName)
  }

  private def predict(servingClient: ServingClient, conf: Configuration): Unit = {
    // 1. prepare data
    val hdfsInputFileName = conf.get(AngelConf.ANGEL_PREDICT_DATA_PATH)
    val dim = conf.getLong(MLConf.ML_FEATURE_INDEX_RANGE, MLConf.DEFAULT_ML_FEATURE_INDEX_RANGE)
    val format = conf.get(MLConf.ML_DATA_INPUT_FORMAT)
    val iterator = Util.read(conf, new Path(hdfsInputFileName), dim, format)

    // 2. prepare output file
    val localOutputFileName = conf.get(AngelConf.ANGEL_SERVING_PREDICT_LOCAL_OUTPUT)
    val lof = new File(localOutputFileName)
    lof.deleteOnExit()
    lof.createNewFile()
    val writer: BufferedWriter = if (localOutputFileName != null && localOutputFileName.length > 0) {
      new BufferedWriter(new FileWriter(localOutputFileName))
    } else null

    // 3. predict
    val modelName = conf.get(AngelConf.ANGEL_SERVING_MODEL_NAME)
    val coordinator = servingClient.getModel(modelName).get.getCoordinator()

    var count = 0
    var start = System.currentTimeMillis()
    while (iterator.hasNext && servingClient.isServable(modelName)) {
      val rowVector = new PredictData[TVector](iterator.next())
      val predres = coordinator.predict(rowVector)
      if (writer != null) {
        writer.write(predres.score.toString)
        writer.newLine()
      }
      count += 1
      if (count % 500 == 0 && count != 0) {
        val elapsed = System.currentTimeMillis() - start
        println(s"500 sample were predicted, the total sample are $count, the elapsed time is $elapsed ")
        start = System.currentTimeMillis()
      }
    }
    if (writer != null) writer.close()

  }

  def main(args: Array[String]): Unit = {
    val conf = getConf(args)

    // 1. create servingClient
    val servingClient = createServingClient(conf)

    // 2. retrieve model
    val model = retrieveModel(servingClient, conf)
    if (model.isDefined) {
      println("model get success!")
    } else {
      println("model get failure!")
      System.exit(0)
    }

    // 3. predict
    predict(servingClient, conf)
  }
}

package com.tencent.angel.serving.laucher

import java.net.InetSocketAddress

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.serving.client.ServingClient
import com.tencent.angel.utils.ConfUtils
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat

object DistributeModelLoader {
  private val LOG: Log = LogFactory.getLog(DistributeModelLoader.getClass)

  private def getConf(args: Array[String]): Configuration = {
    val conf = ConfUtils.initConf(args)

    val dummySpliter = conf.get(AngelConf.ANGEL_AM_USE_DUMMY_DATASPLITER)
    if (dummySpliter == null || dummySpliter.isEmpty) {
      conf.setBoolean(AngelConf.ANGEL_AM_USE_DUMMY_DATASPLITER, true)
    }

    val jobOpDeleteOnExist = conf.get(AngelConf.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST)
    if (jobOpDeleteOnExist== null || jobOpDeleteOnExist.isEmpty) {
      conf.setBoolean(AngelConf.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST, true)
    }

    val inputFormatClass = conf.get(AngelConf.ANGEL_INPUTFORMAT_CLASS)
    if (inputFormatClass == null || inputFormatClass.isEmpty) {
      conf.set(AngelConf.ANGEL_INPUTFORMAT_CLASS, classOf[CombineTextInputFormat].getName)
    }

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

  private def loadModel(servingClient:ServingClient, conf:Configuration): Unit = {
    val modelpath = conf.get(AngelConf.ANGEL_LOAD_MODEL_PATH)
    val replica = conf.getInt(AngelConf.ANGEL_SERVING_SHARDING_NUM, 3)
    val concurrent = conf.getInt(AngelConf.ANGEL_SERVING_SHARDING_CONCURRENT_CAPACITY, 3)
    val modelName = conf.get(AngelConf.ANGEL_SERVING_MODEL_NAME)

    val shardingModelClass = conf.get(AngelConf.ANGEL_SERVING_SHARDING_MODEL_CLASS)
    val seemScuess = servingClient.loadModel(modelName, modelpath, replica, concurrent, shardingModelClass)
    if (!seemScuess) {
      LOG.error(s"It seem there already is a model with the name of $modelName, pls. change a name and retry!")
      println(s"It seem there already is a model with the name of $modelName, pls. change a name and retry!")
      System.exit(0)
    }

    var wait: Long = 0
    val timeout: Long = conf.getLong(AngelConf.ANGEL_SERVING_MODEL_LOAD_TIMEOUT_MINUTE, 10) * 60 * 1000
    val interval: Long = conf.getLong(AngelConf.ANGEL_SERVING_MODEL_LOAD_CHECK_INTEVAL_SECOND, 30) * 1000
    while (!servingClient.isServable(modelName) && wait < timeout) {
      wait += interval
      println("Server is not servable, pls. wait ...")
      Thread.sleep(interval)
    }
    if (!servingClient.isServable(modelName)) {
      println("Server is still not servable after many retry, there may be a problem!")
      servingClient.unloadModel(modelName)
      System.exit(0)
    }
    println("\nload model finished!")
  }

  private def unloadModel(servingClient:ServingClient, conf:Configuration):Unit = {
    val modelName = conf.get(AngelConf.ANGEL_SERVING_MODEL_NAME)
    val seemScuess = servingClient.unloadModel(modelName)
    if (!seemScuess) {
      LOG.info(s"the $modelName is not exist, pls. check !")
      println(s"the $modelName is not exist, pls. check !")
      System.exit(0)
    }
  }

  def main(args: Array[String]): Unit = {
    // 1. parse arguments
    LOG.info("Parse arguments for oloding/unloding model ...")
    val conf = getConf(args)

    // 2. create servingClient
    LOG.info("Create servingClient ...")
    val servingClient = createServingClient(conf)

    LOG.info("Create servingClient ...")
    val flag = conf.get(AngelConf.ANGEL_SERVING_MODEL_LOAD_TYPE)
    val modelName = conf.get(AngelConf.ANGEL_SERVING_MODEL_NAME)
    if (flag.equalsIgnoreCase("load")){
      // 3. load model
      LOG.info(s"$modelName is loading ...")
      loadModel(servingClient, conf)
      LOG.info(s"$modelName loading finished !")
    } else if (flag.equalsIgnoreCase("unload")) {
      // 4. unload model
      LOG.info(s"$modelName is unloading ...")
      unloadModel(servingClient, conf)
    } else {
      println("Unknow parameter!")
      System.exit(0)
    }
  }

}

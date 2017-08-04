### GlobalMetrics
1.1.0 版本的MLLearner类增加了一个重要的成员globalMetrics：GlobalMetrics，它用于收集所有task的局部算法指标，在master汇总，得到全局指标。全局指标会打印在client的日志里、并实时写入"angel.log.path"参数指定的HDFS路径。

* GlobalMetrics类    
  这个类用于收集各个task的指标，首先定义要收集的指标，然后在迭代中添加指标值到对应的指标。其主要成员、方法如下：
    * **metricsTable：Map[String, Metric]**    
      metricsTable存储要收集的指标，key=指标名，value=指标，是Metric类的实例。
    * **addMetrics(metricName: String, metric: Metric)**    
      addMetrics 方法向 metricsTable 添加指标，metricName和metric对应。如下所示，添加指标"train.loss"、"validation.loss"，两个指标都是LossMetric。
        ```java
        // Add "train.loss" and "validation.loss" metric to globalMetrics
        // Both of them are LossMetric
        globalMetrics.addMetrics("train.loss", LossMetric(trainData.size))
        globalMetrics.addMetrics("validation.loss", LossMetric(validationData.size))
        ```

    * **metrics(metricName: String, metricValues: Double\*)**    
      metrics方法向 metricsTable 中 metricName 对应的指标添加值。如下所示：
        ```java
        // Add train loss value to "train.loss"
        globalMetrics.metrics("train.loss", trainLossValue)
        // Add validation loss value to "validation.loss"
        globalMetrics.metrics(MLConf.TRAIN_LOSS, validationLossValue)
        ```
    
* Metric类   
    Metric类是指标类，包含指标数值汇总方法merge和指标计算方法calculate。
    * **merge(other:Metric)**    
        每次迭代完成后，master 调用 merge 方法汇总所有task的指标。    
    * **calculate**  
        每次迭代完成后，master 调用 calculate 方法，计算全局指标

    要实现一个具体的指标类需要继承Metric类并实现merge、calculate等方法。以计算单个样本平均loss值的LossMetric为例：
    * **LossMetric**
        * var sampleNum:Int  
          每个task上的样本数量
        * var globalLoss: Double
          每个task上的loss值
        * merge(other: Metric)  
          master 汇总所有task的sampleNum、globalLoss值，汇总方式为累加。
          ```java
          override def merge(other: Metric): Metric = {
            this.sampleNum += other.asInstanceOf[LossMetric].sampleNum
            this.globalLoss += other.asInstanceOf[LossMetric].globalLoss
            this
          }
          ```
        * calculate   
          master 汇总数值后，计算全局指标，计算方法为全局globalLoss值除样本数：
          ```java
          override def calculate: Double = {
          this.globalLoss / this.sampleNum
          }
          ```
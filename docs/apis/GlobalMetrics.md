# GlobalMetrics（全局指标）


> 在一个算法的运行训练过程中，会有各种各样的指标，而这些指标，在单独的一个Worker上的变化，是没有太大意义的，往往需要全局汇总，从整体上来观察这些值的变化。例如Loss，AUC…… 这些是分布式机器学习框架需要提供的必备功能，Angel也不例外，GlobalMetrics就是为此目的而设计的。

## 说明

GlobalMetrics，用于收集所有task的局部算法指标，在Master汇总，得到全局指标。全局指标有两个输出点：

1. Client日志的Info
2. "angel.log.path"参数指定的HDFS路径。

## 用法

全局指标的使用非常简单，只有一个类，那就是GlobalMetrics，分成**收集**和**透出**两个阶段

1. **收集**

	收集发生于各个Task内，一般写于Learner类。首先定义要收集的指标，然后在迭代中添加指标值到对应的指标。其主要成员、方法如下：

	* **addMetrics(metricName: String, metric: Metric)**

		添加指标，指定metricName和metric类型。这个方法只需要在迭代前调用一次，写到循环外面。

	 	如下所示，添加指标"train.loss"、"validation.loss"，两个指标都是LossMetric

    ```java
	// Add "train.loss" and "validation.loss" metric to globalMetrics
	// Both of them are LossMetric
	globalMetrics.addMetrics("train.loss", LossMetric(trainData.size))
	globalMetrics.addMetrics("validation.loss", LossMetric(validationData.size))
    ```

	* **metric(metricName: String, metricValue: Double)**
	     记录指标值。往metricName对应的指标中，添加值。如下所示：

    ```java
	// Add train loss value to "train.loss"
	globalMetrics.metrics("train.loss", trainLossValue)
	globalMetrics.metrics("validation.loss", validLossValue)
    ```

2. **透出**

	Metrics的**透出**发生于Client端，但是这个过程是不需要手工触发的，只要你在Task中的Learner进行了收集，AngelClient就能自动感知，并进行输出，默认格式如下：

    	Epoch=1 Metrics={"train.loss":0.613, "validation.loss": 0.513}
    	Epoch=2 Metrics={"train.loss":0.622, "validation.loss": 0.521}
    	Epoch=3 Metrics={"train.loss":0.635, "validation.loss": 0.585}


	同时，在指定的HDFS目录，将会有日志输出，指定参数为：

    ```java
      public static final String ANGEL_LOG_PATH = "angel.log.path";
    ```

### 扩展

目前系统只提供了标准的几种Metric，包括：

* LossMetric
* ObjMetric
* ErrorMetric

如果有需要的话，用户可以方便的，针对自己的算法，自定义**Metric**类进行扩展。自定义一个Metric类，需要实现如下两个方法：

* **merge(other:Metric)**
	每次迭代完成后，master 调用 merge 方法汇总所有task的指标。
* **calculate**
	每次迭代完成后，master 调用 calculate 方法，计算全局指标

以计算单个样本平均loss值的LossMetric为例：

  ```java
	class LossMetric(var sampleNum:Int) extends Metric {
		……
	// 汇总各个Task的sampleNum、globalLoss值，汇总方式为累加。
	override def merge(other: Metric): Metric = {
	  this.sampleNum += other.asInstanceOf[LossMetric].sampleNum
	  this.globalLoss += other.asInstanceOf[LossMetric].globalLoss
	  this
		}

	// 计算全局指标，计算方法为全局globalLoss值除样本数：
	override def calculate: Double = {
		this.globalLoss / this.sampleNum
	 }
  ```

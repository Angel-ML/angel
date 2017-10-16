# GlobalMetrics


>  For model training purpose, we want to track various metrics for evaluating the performance of the model, (such as the training loss, AUC, etc.). A metric is only sensible when calculated using data across all partitions. A fundamental functionality of any distributed machine-learning framework is to assemble metrics from individual workers to generate a global result, and GlobalMetrics is designed to achieve this goal for Angel.

In calculating a metric, GlobalMetrics assembles results from all individual tasks to the master, generates a global result and output to two places:

1. Info section of log on the client side
2. The HDFS path as specified in "angel.log.path"

## Usage

There is only one single GlobalMetrics class, and it addresses two things in monitoring the metrics: **collecting metrics** and **instrumentation**.

1. **Metric Collecting**

	This process occurs within a task, usually implemented in the Learner class. It defines and registers the metrics, and adds and updates their values during the training iterations. There are two methods defined for this purpose:

	* **addMetrics(metricName: String, metric: Metric)**

		To add metrics, specify metricName and metric type. This method needs to be called only once, before the iterations (i.e. outside the loop).

	 	We show an example below of how to register two metrics of the LossMetric type, "train.loss" and "validation.loss":

    ```java
	// Add "train.loss" and "validation.loss" metric to globalMetrics
	// Both of them are LossMetric
	globalMetrics.addMetrics("train.loss", LossMetric(trainData.size))
	globalMetrics.addMetrics("validation.loss", LossMetric(validationData.size))
    ```

	* **metric(metricName: String, metricValue: Double)**
	     This method adds a value to the metric registered under metricName, as follows:

    ```java
	// Add train loss value to "train.loss"
	globalMetrics.metrics("train.loss", trainLossValue)
	globalMetrics.metrics("validation.loss", validLossValue)
    ```

2. **Metric Instrumentation**

	This process is on the client side. It does not need to be triggered manually: as long as metrics are registered by `Learner` for a task, AngelClient automatically knows it and logs metrics in the following format:

    	Epoch=1 Metrics={"train.loss":0.613, "validation.loss": 0.513}
    	Epoch=2 Metrics={"train.loss":0.622, "validation.loss": 0.521}
    	Epoch=3 Metrics={"train.loss":0.635, "validation.loss": 0.585}


	Meanwhile, logs are written to the specified HDFS directory:

    ```java
      public static final String ANGEL_LOG_PATH = "angel.log.path";
    ```

### Extension

Currently, Angel pre-defined only a couple of standard metric types, including:

* LossMetric
* ObjMetric
* ErrorMetric

Users can define and implement their only **Metric** class with the following two methods: 

* **merge(other:Metric)**
	The master calls this method to merge results from all tasks after each iteration. 
* **calculate**
	The master calls this method to calculate a global result of the metric after each iteration.

Let's look at an example where we calculate the average loss per sample (LossMetric-type):

  ```java
	class LossMetric(var sampleNum:Int) extends Metric {
		……
	// merge sampleNum and globalLoss from each task by aggregating
	override def merge(other: Metric): Metric = {
	  this.sampleNum += other.asInstanceOf[LossMetric].sampleNum
	  this.globalLoss += other.asInstanceOf[LossMetric].globalLoss
	  this
		}

	// calculate the global metric by dividing globalLoss by number of samples:
	override def calculate: Double = {
		this.globalLoss / this.sampleNum
	 }
  ```

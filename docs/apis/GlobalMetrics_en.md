# GlobalMetrics


> There are many metrics used for model training (such as training loss, AUC, etc.). A metric is only sensible when calculated using data across all partitions. A fundamental functionality of any distributed machine-learning framework is to assemble metrics from individual workers to generate a global result, and GlobalMetrics is designed to achieve this for Angel.


## Introduction

In calculating a metric, GlobalMetrics assembles results from all individual tasks to the master, generates a global result and outputs to two places:

1. Info of the client's log
2. The HDFS path as specified in "angel.log.path"

## Usage

There is only one single GlobalMetrics class with two methods to **assemble** and **output**.

1. **Merge**

	This process is defined for a task, usually implemented in the Learner class. Define the metric, and simply add value to the corresponding metric during the iterations, done with the following methods:

	* **addMetrics(metricName: String, metric: Metric)**

		To add metrics, specify metricName and metric type. This method is called only once before iterations (outside the loop).

	 	We show an example below of how to add two LossMetric type metrics, "train.loss" and "validation.loss" to GlobalMetrics:

    ```java
	// Add "train.loss" and "validation.loss" metric to globalMetrics
	// Both of them are LossMetric
	globalMetrics.addMetrics("train.loss", LossMetric(trainData.size))
	globalMetrics.addMetrics("validation.loss", LossMetric(validationData.size))
    ```

	* **metric(metricName: String, metricValue: Double)**
	     Now we add a value to the metric with the metricName:

    ```java
	// Add train loss value to "train.loss"
	globalMetrics.metrics("train.loss", trainLossValue)
	globalMetrics.metrics("validation.loss", validLossValue)
    ```

2. **Output**

	**Output** of a metric is at the client side. This process does not need to be triggered manually. As long as metrics are added to task by Learner, AngelClient automatically knows this and outputs metrics in the following format:

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

Users can customize their only **Metric** class by implementing the following two methods: 

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

# Angel Quick Start Guide

---

## Knowledge Preparation

This document helps you start writing programs to run on the Angel-PS architecture. The following know-hows are expected before you start:
  
* Basics Scala or Java programming
* Basics of vector, matrix, and tensor: definitions and operations
* Good understanding of machine learning algorithms -- however, if you don't have it yet, use this document as a start

Let's first review some basics:

* Most machine learning algorithms break down to operations of vectors, matrices, tensors. These mathematical forms are also used to represent data. 

* Angel-PS implements matrix operations on the parameter server (PS). It abstracts the parameter matrix, which is distributed on multiple PS servers, into a PSModel. Defining your PSModel and its calculation suffices a basic algorithm to run on the PS.
  

### Angel-PS Architecture

A simple representation of the Angel-PS architecture is shown below:

* PS consists of multiple machines that store the parameter matrix, executing pull/push operations between nodes and updating

* Each worker is a logical compute node and can run one or more tasks

Training algorithms in machine learning are usually implemented in an iterative fashion; in each iteration, the worker pulls the latest parameters from the PS, updates their values, and pushes to the PS

![](../img/brief_structure.png)


## Start your first Angel algorithm: LR

This example guides you through the implementation of a simple Logistic Regression algorithm. The code can be found in example.quickStart.

Let w denote the 1-by-N parameter matrix (an N-dimensional vector), where N is the number of weights in the LR model. 

We train the LR model with gradient descent algorithm. In each iteration:

        * task pulls the latest model w from the PS
        * calculates the change in gradient △w
        * pushes △w to the PS

We need the following three steps to realize the procedure:

1. **Define the model([MLModel](../apis/MLModel.md))**

	Define a `myLRModel` class that inherits the `MLModel` class, add an N-dimensional `PSModel` to  `myLRModel` using the `addPSModel` method, and set the save path for the LR model using the `setSavePath` method.
	
	The value of N, save path, etc. can be configured through `conf`.
	 
	

	```Scala
	class myLRModel(ctx: TaskContext, conf: Configuration) extends MLModel(ctx){
		
		val N = conf.getInt(MLConf.ML_FEATURE_NUM, MLConf.DEFAULT_ML_FEATURE_NUM)

		val weight = PSModel[DenseDoubleVector]("mylr.weight", 1, N)
		weight.setAverage(true)
		addPSModel(weight)

		 override 
		 def setSavePath(conf: Configuration): Unit = {
			val path = conf.get(AngelConfiguration.ANGEL_SAVE_MODEL_PATH)
			if (path != null) weight.setSavePath(path)
		  ｝
	}
	```
2. **Define the Task([TrainTask](../apis/Task.md))**

	Angel's model training is done through tasks. We need to define `myLRTrainTask` to train the LR model.

	`myLRTrainTask` needs to inherit the `TrainTask` class and implement the following two methods:

	* **Data Parsing**    

	Before training, each line of text in the input is parsed into a training datum, implemented in the `parse` method. Here, we use `DataParser` to parse the data in dummy format.

	```
	  override
	  def parse(key: LongWritable, value: Text): LabeledData = {
	    DataParser.parseVector(key, value, feaNum, "dummy", negY = true)
	  }
	```

	You can access the preprocessed data through the task's `dataBlock`.

	* **Training**

	Angel will automatically run the `train` method in any `TrainTask` subclass. We need to implement the `train` method for `myLRTrainTask`.

	In this simple LR example:

        * We create an instance of the `myLRModel` class, and then start the iterations. 
        * In each iteration, 

		* Task pulls the weight parameters from the PS
		* Workers calculate gradient `grad` and push it to the PS; PS then automatically updates the weight parameters 
		* Call `clock()`, `incIteration()` after pushing `grad` 

		```Scala
		override
		def train(ctx: TaskContext): Unit = {
			
			// A simple logistic regression model
			val model = new myLRModel(ctx, conf)
			
			// Apply batch gradient descent LR iteratively
			while (ctx.getIteration < epochNum) {
			  // Pull model from PS Server
			  val weight = model.weight.getRow(0)
			
			  // Calculate gradient vector
			  val grad = bathGradientDescent(weight)
			
			  // Push gradient vector to PS Server
			  model.weight.increment(grad.timesBy(-1.0 * lr))
			
			  // LR model matrix clock
			  model.weight.clock.get
			
			  // Increase iteration number
			  ctx.incIteration()
			}
		}
		```
  
3. **Define the Runner([MLRunner](../apis/MLRunner.md))**

	We have already defined the LR model and implemented its training method. The next step is to implement the `Runner` class to submit the training task to the cluster.

	Define `myLRRunner` class that inherits `MLRunner`, implement the `train` method to submit `myLRModel` class and `myLRTrainTask` class.

	
```Scala

	class myLRRunner extends MLRunner{
	  ……
	  override
	  def train(conf: Configuration): Unit = {
	    train(conf, myLRModel(conf), classOf[myLRTrainTask])
	   }
	}
	
```

### Run on Yarn

You can submit the application to Yarn using the sample command below:

```
./bin/angel-submit \
--action.type train \
--angel.app.submit.class com.tencent.angel.example.quickStart.myLRRunner  \
--angel.train.data.path $input_path \
--angel.save.model.path $model_path \
--ml.epoch.num 10 \
--ml.feature.num 10000 \
--ml.data.type dummy \
--ml.learn.rate 0.001 \
--angel.workergroup.number 3 \
--angel.worker.memory.mb 8000  \
--angel.worker.task.number 3 \
--angel.ps.number 1 \
--angel.ps.memory.mb 5000 \
--angel.job.name myLR
```

After submission, follow [Running on Yarn](../deploy/run_on_yarn.md) if you are not yet familiar with Yarn.


----
OK. You have just completed a simple Angel job. Want to write more complex machine learning algorithms? Read the complete [Angel Programming Guide](../programmers_guide/angel_programing_guide.md). 

Welcome to Angel's world. 




# MLRunner

> MLRunner is the entry point to Angel algorithms. It defines the standard process of starting up an Angel application, and encapsulates usage of [AngelClient](AngelClient.md). 

## Functions

* Starts Angel PS, loads and saves model, and starts the default implementations of processes such as task
* In general, tha application can directly call the functions above

## Core Methods

1. **train**
	- Definition: ``` train(conf: Configuration)```
	- Function: starts up Angel app's model training process
	- Parameters: conf: configures Angel job and algorithm
	- Return value: none

2. **train（default implementation）**

	- **Definition**: ```train(conf: Configuration, model: MLModel, taskClass: Class[_ <: BaseTask[_, _, _]]): Unit```

	- **Function**: starts Angel app's model training process. This method encapsulates specific Angel PS/worker starting-up and model-loading/saving processes; can be referenced by subclasses directly

	- **Parameters**
		* conf: configures Angel job and algorithm
		* model: MLModel, the machine-learning model/algorithm
		* taskClass: Class[_ <: TrainTask[_, _, _]] represents the task class of the algorithm 
		* Return value: none

3. **incTrain**

	* **Definition**: ```incTrain(conf: Configuration)```
	- **Function**: updates an existing model using incremental training
	- **Parameters**: conf: configures Angel job and algorithm
	- **Return value**: none

4. **predict**

	- **Definition**: ```predict(conf: Configuration)```
	- **Function**: starts Angel app and computes model prediction
	- **Parameter**: conf: configures Angel job
	- **Return value**: none

5. **predict（default implementation）**

	- **Definition**: ```predict(conf: Configuration, model: MLModel, taskClass: Class[_ <: PredictTask[_, _, _]]): Unit```
	- **Function**: starts Angel app and updates an existing model using incremental training. This method encapsulates specific Angel PS/worker starting-up and model-loading/save processes; can be referenced by subclasses directly
	- **Parameters**: conf: configures Angel job
	- **Return value**: none

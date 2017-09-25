# Task

> Task is Angel's metacomputing class. All the machine-learning algorithms on Angel need to inherit Task to implement the `train` or `predict` processes. Tasks run within the worker and can share certain resources in the worker.

Understanding Task enhances understanding of the programming principles for Angel.

* Task completes two actions: reading data and training; for training, a task is only in charge of the data that it reads
* Intermediate results are neither written to the disk nor exposed, unlike many other systems
* Different Tasks do not exchange data while computing, but only communicate with the PSServer

## Functionality

A Task's execution process is shown in the following chart:

![][1]

Task's basic process consists of two steps:

1. **Reading the training data**
	
	Raw data live on top of the distributed file system (DFS) and are not immediately usable by machine-learning algorithms in their raw format. Angel therefore abstracts out the process of preparing data for training, where Task pulls data to local from DFS, analyzes and transforms data to have the desired structure as DataBlock. This step includes `preProcess` and `parse`.
 
2. **Computing (train or predict)**
	
	Also called the **run** step. Generally, for model training, this step runs the iterative training procedure (thus data are used for computing for many times) and outputs the trained model; for prediction, this step generates prediction using the trained model (thus data are used for computing just once) and outputs the model prediction. 
 
In order for the application to be able to customize its computing procedure, Angel abstracts out **BaseTaskInterface**, and provides base classes such as **BaseTask**, **TrainTask**, and **PredictTask**, which can be extended to fulfill the specific requirements of the application.

Task will need to access the system config information and control the iteration progress during computing, provided by **TaskContext**.

## Core Classes

### **BaseTaskInterface<KEYIN, VALUEIN, VALUEOUT>**

BaseTaskInterface defines the interface to an algorithm's computing procedure. KEYIN and VALUEIN indicate the raw data type; VALUEOUT indicate type of the pre-processed data (input data for training)

1. **parse**
	- Definition: ```VALUEOUT parse(KEYIN key, VALUEIN value)```
	- Functionality: analyzes one row of raw data and structures them for training
	- Parameters: KEYIN key, VALUEIN value, the key-value representation of raw data
	- Return value: VALUEOUT, training data

2. **preProcess**
	- Definition: ```void preProcess(TaskContext taskContext)```
	- Functionality: transforms raw data blocks to the training set
	- Parameters: TaskContext taskContext, contextual information of Task for preprocessing
	- Return value: none

3. **run**
	- Definition: ```void run(TaskContext taskContext) throws AngelException```
	- Functionality: model training process
	- Parameters: TaskContext taskContext, current Task's contextual information
	- Return value: none

In order to further simplify the application's programming interface, Angel defines two subclasses of BaseTask, TrainTask and PredictTask, whose VALUEOUT are both LabeledData, used under the `train` and `predict` modes, respectively. TrainTask and PredictTask can both be extended to address specific requirements of the application.  

### TrainTask[KEYIN, VALUEIN]

1.  **train**
	- Definition: ```void train(TaskContext taskContext)```
	- Functionality: model training process
	- Parameters: TaskContext taskContext, Task's contextual information
	- Return value: none

### PredictTask[KEYIN, VALUEIN]

1. **predict**

	- Definition: ```def predict(taskContext: TaskContext)```
	- Functionality: computes model prediction 
	- Parameters: TaskContext taskContext, current Task's contextual information
	- Return value: none

### TaskContext

The application can get task config and task execution information through TaskContext. In addition, intermediate metrics can be saved in TaskContext and visible in the application UI.

1. **getReader**
	- Definition: ```<K, V> Reader<K, V> getReader()```
	- Functionality: get Reader for the data block assigned to the Task, typically used only in the preProcess interface
	- Parameters: none
	- Return value: Reader for raw data block

2. **getConf**
	- Definition: ```Configuration getConf()```
	- Functionality: get Task config
	- Parameters: none
	- Return value: Task config

3. **getTotalTaskNum**
	- Definition: ```int getTotalTaskNum()```
	- Functionality: get total number of Tasks
	- Parameters: none
	- Return value: total number of Tasks

4.  **getIteration**
	- Definition: ```int getIteration()```
	- Functionality: get the iteration index of current Task
	- Parameters: none
	- Return value: iteration index of the current Task
	
5. **incIteration**
	- Definition: ```void incIteration()```
	- Functionality: increase iteration index by 1, meaning entering the next iteration
	- Parameters: none
	- Return value: none

6. **getMatrixClock**
	- Definition: ```int getMatrixClock(int matrixId)```
	- Functionality: get the clock information of the specified matrix, where clock information is used for sync control between Tasks
	- Parameters: int matrixId, matrix id
	- Return value: matrix's current clock

7. **globalSync**
	- Definition: ```void globalSync()```
	- Functionality: sync the current Task with all other Tasks in the job on their progresses. The method is mainly used under the BSP protocol and does not need to be explicitly called by the application (Angel has internal sync control in units of matrices), unless otherwise required specifically by the algorithm.
	- Parameters: none
	- Return value: none

8. **setCounter**
	- Definition: ```void setCounter(String counterName, int updateValue)```
	- Functionality: set Counter value
	- Parameters:
		- String counterName, Counter name
		- int updateValue, Counter value
	- Return value: none

9. **updateCounter**
	- Definition: ```void updateCounter(String counterName, int updateValue)```
	- Functionality: Counter value aggregates
	- Parameters:
		- String counterName, Counter name
		- int updateValue, Counter update value
	- Return value: none


  [1]: ../img/task_execute.png

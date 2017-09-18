# AngelClient

---


> AngelClient encapsulates all the main interfaces to the global operations of Angel, including the direct control over PSServer and worker. MLRunner calls AngelClient to complete the calling process of a machine learning application.

## Functionality

* In general, For a training process, a typical, fixed series of actions of AngelClient include starting PSServer, loading the model, starting the worker to run the specified tasks, waiting for the tasks to complete, and saving the model to the specified location.


## Core Interfaces

1. **startPSServer**
	- Definition: ```void startPSServer()```
	- Functionality: starting Angel parameter servers. The method will start the Angel master and send it the command to start the parameter servers; the method will return when all parameter servers have been started successfully
	- Parameters: none
	- Return value: none

2. **loadModel**
	- Definition: ```void loadModel(MLModel model)```
	- Functionality: loading models. The method has different procedures under different functional modes. Currently, Angel supports the **train** mode and the **predict** mode, whereas **train** can be in either the **new model training** mode or the **incremental training** mode. Under the **incremental training** mode or the **predict** mode, the method will first load the meta data and parameters of a previously trained model into the parameter server's memory, preparing for the next steps of operation. Under the **new model training** mode, the method will define the new model and initialize the model parameters in the parameter server directly
	- Parameters:
		- MLModel model: contains the model partition information, model initialization methods, etc.
	- Return value: none

3.  **runTask**
	- Definition: ```void runTask(Class<? extends BaseTask> taskClass) ```
	- Functionality: starting the computing process. The method starts worker and task to start the specific computing process
	- Parameters:
		- Class<? extends BaseTask> taskClass: task operational procedure; in general, every algorithm in Angel MLLib provides a corresponding task implementation, but the users need to implement customized versions if they need to modify the specific procedure or implement a new algorithm
	- Return value: none
	
4. **waitForCompletion**

	- Definition: ```void waitForCompletion() ```
	- Functionality: waiting for operations to complete. The method has different procedures under **train** and **predict**
		- under the **train** mode: the method returns when the specific training process finishes
		- under the **predict** mode: when the prediction process finishes, the method renames the prediction result that is saved under the temporary output dir to the final output dir
	- Parameters: none
	- Return value: none

5. **saveModel**
	- Definition: ```void saveModel(MLModel model) ```
	- Functionality: saving the trained model. The method is only valid under the **train** mode
	- Parameters: none
	- Return value: none

6. **stop**
	- Definition: ```void stop() ```
	- Functionality: stopping the application, releasing the computing resources and cleaning up the temporary directories
	- Parameters: none
	- Return value: none

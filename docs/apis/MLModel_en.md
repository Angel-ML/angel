# MLModel

---

> MLModel is an abstract class that serves as the parent class for all models on Angel. It contains all the required methods for implementing a machine-learning algorithm on Angel. A complex machine-learning algorithm usually needs multiple [PSModel](PSModel_en.md) to work together, and MLModel provides a unified model that is convenient to handle in this process.  

## Functionality

MLModel is an abstract class that needs to be inherited to implement a specific machine-learning algorithm. It is a container class that manages all the PSModels in the algorithm, allowing them to be loaded, trained and saved as a unified model. In the meantime, MLModel contains the logic of `predict`. 


## Core Methods


*  **predict**
	- Definition: ```def predict(storage: DataBlock[predictType]): DataBlock[PredictResult]```
	- Functionality: get predictions based on the machine-learning model and test data; procedurs are implemented in specific PSModels
	- Parameters: storage: DataBlock[predictType], test data
	- Return value: DataBlock[PredictResult], prediction result

*  **setSavePath**

	- Definition: ```def setSavePath(conf: Configuration)```
	- Functionality: set save path for the model
	- Parameters: conf: Configuration, job configuration
	- Return value: none

  * **setLoadPath**
	- Definition: ```def setLoadPath(conf: Configuration)```
	- Functionality: set load path for model
	- Parameters: conf: Configuration, job configuration
	- Return value: none

*  **addPSModel**

	- Definition: ```def addPSModel(name: String, psModel: PSModel[_])```
	- Functionality: add a new PSModel to MLModel 
	- Parameters:
		- name: String, name of the model
		- psModel: PSModel[_], the corresponding PSModel object
	- Return value: none

* **getPSModel**

	- Definition: ```def getPSModel(name: String): PSModel[_]```
	- Functionality: get the corresponding PSModel object given a model (matrix) name 
	- Parameters: name: String, name of the model (matrix)
	- Return value: a PSModel object


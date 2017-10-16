# MLModel

---

> MLModel is the complete model of a machine-learning algorithm. It serves as the base class for all machine learning models on Angel. A complex machine-learning algorithm usually needs multiple [PSModel](PSModel_en.md) to work together, and MLModel provides unified handling of these PSModels.

## Functionality

MLModel is an abstract class and needs to be inherited in implementing a specific machine-learning algorithm. It is a container class that handles all the PSModels in the algorithm; all the PSModels in it will be loaded, trained and saved as an overall model. In addition, MLModel contains the logic of the `predict` method.


## Core Methods


*  **predict**
	- Definition: ```def predict(storage: DataBlock[predictType]): DataBlock[PredictResult]```
	- Functionality: get model predictions (given a trained model); procedures of this method need to be implemented in specific PSModel
	- Parameters: storage: DataBlock[predictType], data
	- Return value: DataBlock[PredictResult], prediction result on provided data

*  **setSavePath**

	- Definition: ```def setSavePath(conf: Configuration)```
	- Functionality: set save path for model
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
		- name: String, PSModel name
		- psModel: PSModel[_], PSModel object
	- Return value: none

* **getPSModel**

	- Definition: ```def getPSModel(name: String): PSModel[_]```
	- Functionality: retrieve PSModel object with its name
	- Parameters: name: String, PSModel name
	- Return value: a PSModel object


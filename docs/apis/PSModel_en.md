# PSModel

---

> PSModel is Angel's core abstract class. It encapsulates the Context and Client details of the remote parameter server and provides the commonly-used interfaces for accessing and updating remote matrices and vectors, allowing the algorithm engineers to operate the distributed matrices and vectors on the parameter server as if operating local objects. PSModel is a mutable model object that can be updated iteratively.

## Functionality

PSModel has three core classes: **MatrixContext，MatrixClient，TaskContext**, capable of any operations to the remote parameter server.

For developing machine-learning algorithms on Angel, we recommend creating PSModel through MLModel and working on top of it. PSModel has the following five major interfaces:

1. **[pull-type](#1-pull-type)**
	* getRow
	* getRows
	* get(func: GetFunc)
	
2. **[push-type](#2-push-type)**
	* increment
	* update(func: UpdateFunc)

3. **[sync-type](#3-sync-type)**
	* syncClock
	* clock
	* flush

4. **[io-type](#4-io-type)**
	* setLoadPath
	* setNeedSave
	* setSavePath

5. **[behavior-type](#5-behavior-type)**
	* setAverage
	* setHogwild
	* setOplogType
	* setRowType

With reasonable ways of initializing PSModel, setting PSModel's behavioral properties and calling PSModel methods, algorithm engineers can operate the remote distributed model (Martrix or Vector) and programming distributed machine-learning algorithms without worrying about low-level details.


## Core Interfaces


### 0. constructor

* **constructor**

	- **Definition**: ```  def apply[K <: TVector](modelName: String, row: Int, col: Int, blockRow: Int = -1, blockCol: Int = -1)(implicit ctx:TaskContext) ```
	- **Parameters**:
		- modelName: String, model name
		- row: Int, number of rows of matrix
		- col: Int, number of columns of matrix
		- blockRow: Int, number of rows of one block
		- blockCol: Int, number of columns of one block
		-  **ctx: TaskContext**, context of the PSModel Task
			* a PSModel object needs to be bound to a Task since PSModel runs on Worker, as well as supporting the BSP and SSP sync models
			* implicit conversion is used: as long as a ctx object exists in the container of PSModel, it will be automatically injected into the PSModel, with no need of explicit calls)

### 1. pull-type


* **getRow**

	- **Definition**: ```def getRow(rowId: Int): K```
	- **Functionality**: retrieve a specified row from the matrix. Under different sync protocols, this method has different procedures. Angel supports three sync protocols, namely, **BSP**，**SSP** and **ASP**
		* under **BSP** and **SSP**, this method first checks whether the specified row is in local cache **and** whether its clock is current under the sync protocol in use, if false, the method requests the specified row from the PS; if the row's clock on the PS side is not current under the sync protocol (with its specific staleness criterion), this method waits until it becomes current
		* under **ASP**, the method directly requests the specified row from the PS without checking clock
	- **Parameters**:
		- rowId: Int
	- **Return value**: the specified row vector

* **getRows**

	- **Definition**: ```def getRows(rowIndexes:Array[Int]): List[K]```
	- **Functionality**: retrieve specified rows from the matrix. This method works in a similar fashion under different sync protocols as the `getRow` method
	- **Parameters**:
		- rowIndexes: Array[Int]
	- **Return value**: list of the specified row vectors; the list is sorted, consistent with the rowIndexes array

* **getRowsFlow**

	- **Definition**: ```def getRowsFlow(rowIndex: RowIndex, batchNum: Int): GetRowsResult```
	- **Functionality**: retrieve specified rows from the matrix in pipelined fashion (flow); the method returns immediately, allowing simultaneous computing and row retrieving; the method has similar procedures as in `getRow` under the BSP/SSP/ASP protocols
	- **Parameters**:
		- rowIndex: RowIndex
		- batchNum: Int, number of rows that RPC requests every time, defining the granularity of the flow; if set to -1, the system will decide the exact number
	- **Return value**:一a row vector queue; higher-level applications can get the row vectors that are already retrieved from the queue

* **get**

	- **Definition**: ```def get(func: GetFunc): GetResult```
	- **Functionality**: use `psf get` function to get matrix elements or their statistics. Different from getRow/getRows/getRowsFlow, this method only supports ASP protocol
	- **Parameters**:
		- func: GetFunc, get-type psf, where psf is an extension interface for Angel PS
	- **Return value**: GetResult returned by `psf get`


### 2. push-type

* **increment**

	- **Definition**: ```def increment(delta: TVector)```
	- **Functionality**: incrementally update a row of the matrix. This method works under ASP; it caches the delta vector to local, and only directly pushes to the PS when executing `flush` or `clock`
	- **Parameters**: delta: TVector, delta (update) vector, which has the same size as a row vector
	- **Return value**: none

*  **increment**

	- **Definition**: ```def increment(deltas: List[TVector])```
	- **Functionality**: incrementally update some rows of the matrix. This method works under ASP; it caches the delta vector to local, and only directly pushes to the PS when executing `flush` or `clock`
	- **Parameters**: deltas: List[TVector], list of delta vectors, where each delta vector has the same size as a row vector
	- **Return value**: none

*  **update**
	- **Definition**: ```def update(func: UpdaterFunc): Future[VoidResult]```
	- **Functionality**: use `psf update` function to update the parameter matrix. Unlike the `increment` method, this method directly pushes delta to PS
	- **Parameters**: func: GetFunc `psf update` function. For a detailed introduction to psf function, please refer to [psFunc Developing Guide](../design/psf_develop_en.md). Users can customize the psf update function. Angel also provides an [update lib](../design/psf_update_en.md) of commonly-used functions. Unlike the `increment` methods, this method immediately pushes the delta to the PS
	- **Return value**: Future[VoidResult], return value of the psf update function; the application can decide whether to wait for update


### 3. sync-type

*  **syncClock**
	- **Definition**: ```def syncClock():```
	- **Functionality**: simplified version of clock; encapsulates clock().get(). We recommend calling this method unless waiting is necessary
	- **Parameters**: none

*  **clock**
	- **Definition**: ```def clock(): Future[VoidResult]```
	- **Functionality**: update and merge all matrices in local cache (calling `increment` will create local cache for model delta) and send to the PS, then update the matrix clock
	- **Parameters**: none
	- **Return value**: Future[VoidResult], `clock` operation result; the application can choose whether to wait for `clock` to complete

* **flush**

	- **Definition**: ```def flush(): Future[VoidResult]```
	- **Functionality**: update and merge all matrices in local cache (calling `increment` will create local cache for model delta) and send to the PS
	- **Parameters**: none
	- **Return value**: Future[VoidResult], `flush` operation result; the application can choose whether to wait for `flush` to complete

### 4. io-type

* **setLoadPath**
	- **Definition**: ```def setLoadPath(path: String)```
	- **Functionality**: set path for loading matrix; this method is used for incremental learning or under the `predict` mode --- PS loads the matrix of parameters from file for initializing
	- **Parameters**: path: String, existing save path
	- **Return value**: none

* **setSavePath**

	- **Definition**: ```def setSavePath(path: String)```
	- **Functionality**: set path for saving matrix; under the `train` mode, when training is done, the matrix on PS needs to be saved in file
	- **Parameters**: path: String, existing save path
	- **Return value**: none

### 5. behavior-type

* **setAverage**
	- **Definition**: ```def setAverage(aver: Boolean)```
	- **Functionality**: set whether to divide the `update` parameters by the total number of tasks; this method is used in `increment`, but not in `update`
	- **Parameters**: aver: Boolean, if set to true, divide the parameters by the total number of tasks
	- **Return value**: none

*  **setHogwild**
	- **Definition**: ```def setHogwild(hogwild: Boolean)```
	- **Functionality**: set whether to use the **hogwild** mechanism in storing and updating local matrix; when there are more than 1 worker tasks, **hogwild** can save memory usage; default is true
	- **Parameters**: aver: Boolean, if set to true, use hogwild
	- **Return value**: none

* **setRowType**

	- **Definition**: ```def setRowType(rowType: MLProtos.RowType)```
	- **Functionality**: set type and storage method for matrix's row vector, can be based on the model's characteristics such as its sparsity. Currently, Angel supports int, float and double row element, and sparse and dense models
	- **Parameters**:
		- rowType: MLProtos.RowType, currently support
			* **T\_DOUBLE\_SPARSE**: sparse, double type
			* **T\_DOUBLE\_DENSE**: dense, double type
			* **T\_INT\_SPARSE**: sparse, int type
			* **T\_INT\_DENSE**: dense, int type
			* **T\_FLOAT\_SPARSE**: sparse, float type
			* **T\_FLOAT\_DENSE**: dense, float type
			* **T\_INT\_ARBITRARY**: int type

			Users can choose the most memory-efficient setting based on their own use cases.
	- **Return value**: none

*  **setOplogType**

	- **Definition**: ```def setOplogType(oplogType: String)```
	- **Functionality**: set storage method for model delta; when using `increment`, Angel first caches the delta vector in local, by creating a local matrix of the same size as the matrix to be updated
	- **Parameters**:
		- oplogType: String, currently suppoart
			* **DENSE\_DOUBE**: use a dense, double-type matrix for storing the model delta, used when the model matrix to be updated is double-type
			* **DENSE\_INT**: use a dense, int-type matrix for storing the model delta, used when the model matrix is int-type
			* **LIL\_INT**: use a sparse, int-type matrix for storing the model delta, used when the model matrix is int-type, and delta is sparse
			* **DENSE\_FLOAT**: use a dense, float-type matrix for storing the model delta, used when the model matrix is float-type
	- **Return value**: none



*  **setAttribute**
	- **Definition**: ```def setAttribute(key: String, value: String)```
	- **Functionality**: Angel's self-defined extension for matrix parameters
	- **Parameters**:
		* key: String, parameter name
		* value: String, parameter value
	- **Return value**: none

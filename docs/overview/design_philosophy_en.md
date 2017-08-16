# Angel's Design Philosophy

## PS Service

Angel can run in two modes: `ANGEL_PS` & `ANGEL_PS_WORKER`

* **ANGEL_PS**: under the PS Service mode, Angel only starts up the master and PS, but leaves the concrete computations to other frameworks such as Spark, Tensorflow, etc. Angel is only responsible of providing the Parameter Server functionality. 

* **ANGEL_PS_WORKER**: under this mode, Angel starts up the master, PS and worker(s) to complete the computations (such as model training) independently.

## Synchronization Model

* Angel supports multiple synchronization protocols: beside the common **BSP** (Bulk Synchronous Parallel) model, **SSP** (Stale Synchronous Parallel) and **ASP** (Asynchronous Parallel) are also supported to solve the problem of waiting for all tasks to finish in every iteration. 

## Good Scalability

* **psf(ps function)**: In order to meet the specific needs that various algorithms may have toward the parameter server, Angel has abstracted out the parameter requesting and updating processes, providing the psf feature. Users just need to implement their own logic of requesting and updating the parameters, inheriting the psf interface provided by Angel, so as to customize the PS interface without changing Angel's source code. 

* **User-defined data format**: Angel supports Hadoop's InputFormat interface, allowing convenient, customized implementation of the data-file format.

* **User-defined model partitioner**: By default, Angel partitions the model (matrix) into rectangular parts of equal size; users can also customize the partitioner class to obtain the desired partitions. 

## Usability

* **Automatic partitioning of training data and model**: Angel automatically slices the training data based on the number of workers and tasks; similarly, it automatically partitions the model based on the model size and the number of PS instances.

* **Easy-to-use programming interfaces**: MLModel/PSModel/AngelClient

## Fault-Tolerance Design and Stability 

* **PS fault-tolerance**

	We use the checkpoint method to build fault-tolerant PS. The parameter partitions loaded on the PS are written into hdfs periodically. If one PS instance dies, the master will start up a new PS instance, which then loads the most recent checkpoint written by the previous PS instance and resume the service. The advantage of this method is its simplicity that relies on the hdfs's replica creation method for fault tolerance; the disadvantage, on the other hand, is to lose a small amount of parameter updates almost inevitably.  

* **Worker fault-tolerance**

	When a worker instance dies, the master will start up a new worker instance, which then retrieves the current status (such as the iteration index) from the master and requests the most updated model parameters from the PS, to resume the iterations. 

* **Master fault-tolerance**

	The Angel master periodically writes the job status into hdfs. On Yarn's side, when ApplicationMaster dies, ResourceManager can restart the ApplicationMaster up to a given number of retry attempts; relying on this retry mechanism of Yarn, when Angel master dies, Yarn will restart a new Angel master, which can then load the job status and restart workers and PS, resuming the computations. 

* **Slow-worker detection**

	The Angel master will collect some metrics of worker performance. Workers that are significantly slower than average, once detected by the master, will be rescheduled to other machines so that they don't slow down the entire application. 


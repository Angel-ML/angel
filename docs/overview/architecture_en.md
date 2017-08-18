# Angel's Architecture Design

----

![][1]

The overall design of Angel is simple, clear-cut, easy-to-use, without excessive complications; it focuses on the machine-learning model-related characteristics and pursues the best performance of high-dimensional, complex models. Angel's architecture design consists of three modules: 
1. **Parameter Server Layer** provides a common `Parameter Service` (PS) service, responsible for the distributed model storage, communication synchronization and coordination of computing, providing `PS Service` through PSAgent2. **Worker Layer** consists of distributed compute nodes based on Angel's model design, which automatically read and partition data, compute the model updates locally, communicate with `PS Server` through `PS Client`, and complete model training or generating predictions. One worker contains one or more tasks, where a task is a computing unit in Angel. Designed this way, the tasks are able to share many public resources that a worker has acess to3. **Model Layer** is a virtue layer that is abstract rather than physical, hosting model pull/push operations, multiple sync protocols, model partitioner, psFunc...bridging between the worker layer and the PSServer. 
 
In addition to the three modules, there are two important classes that deserve attention, though not shown in the chart:

1. **Client**: The Initiator of Angel Application

	* Start/stop PSServer
	* Start/stop Angel worker
	* Load/save the model
	* Start the specific computation
	* Obtain application status


2. **Master**: The Maintainer of Angel Application

	* Slice and distribute data and the parameter matrix 
	* Request computing resources for worker and PSServer from Gaia
	* Coordinate, manage and monitor worker and PSServer

With the above design, Angel's overall architecture is comparatively easy to scale up. 

* **PSServer Layer**: provide flexible PS support for multiple frameworks through PS-Service
* **Model Layer**: provide necessary functions of PS and support targeted optimization for performance
* **Worker Layer**: satisfy needs for algorithm development and innovation based on independent API 

Therefore, the distributed computing engineers can optimize the core layers, whereas the algorithm engineers and data scientists can reuse these results, focusing on the maths and implementations of models/algorithms to pursue the best performance and accuracy. 


[1]: ../img/angel_architecture_1.png

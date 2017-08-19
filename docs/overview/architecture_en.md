# Angel's Architecture Design

----

![][1]

The overall design of Angel is simple, clear-cut, easy-to-use, without excessive complications; it focuses on characteristics related to machine learning models and pursues the best performance of high-dimensional models. Angel's architecture design consists of three modules: 
1. **Parameter Server Layer** provides a common `Parameter Server` (PS) service, responsible for distributed model storage, communication synchronization and coordination of computing, also provide `PS Service` through PSAgent.2. **Worker Layer** consists of distributed compute nodes designed based on the Angel model, which automatically read and partition data, compute the model updates locally, communicate with the `PS Server` through the `PS Client`, and complete the model training and prediction generation. One worker contains one or more tasks, where a task is a computing unit in Angel; designed this way, the tasks are able to share many public resources that a worker has access to.3. **Model Layer** is an abstract, virtue layer without physical components, which hosts functionalities such as model pull/push operations, multiple sync protocols, model partitioner, psFunc, among others; this layer bridges between the worker layer and the PS layer.
 
In addition to the three modules, there are two important classes that deserve attention, though not shown in the chart:

1. **Client**: The Initiator of Angel Application

	* Start/stop PSServer
	* Start/stop Angel worker
	* Load/save the model
	* Start the specific computating process
	* Obtain application status


2. **Master**: The Guardian of Angel Application

	* Slice and distribute raw data and the parameter matrix 
	* Request computing resources for worker and parameter server from Gaia
	* Coordinate, manage and monitor worker and PSServer activities

With the above design, Angel's overall architecture is comparatively easy to scale up. 

* **PSServer Layer**: provide flexible, multi-framework PS support through PS-Service
* **Model Layer**: provide necessary functions of PS and support targeted optimization for performance
* **Worker Layer**: satisfy needs for algorithm development and innovation based on independent API 

Therefore, the distributed computing engineers can focus on optimization of the core layers, whereas the algorithm engineers and data scientists can focus on the algorithm development and implementation, making full use of the platform, to pursue the best performance and accuracy. 


[1]: ../img/angel_architecture_1.png

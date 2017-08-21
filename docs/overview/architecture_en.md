# Angel's Architecture Design

----

![][1]

The overall design of Angel is simple, clear-cut, easy-to-use, without excessive complex design. It focuses on characteristics related to machine learning and models,  pursuing best performance of high-dimensional models. In
 general, Angel's architecture design consists of three modules:

1. **Parameter Server Layer**: provides a common `Parameter Server` (PS) service, responsible for distributed model storage, communication synchronization and coordination of computing, also provide `PS Service` through PSAgent.

2. **Worker Layer**: consists of distributed compute nodes designed based on the Angel model, which automatically read and partition data and compute model delta locally. It communicate with the `PS Server` through the `PS Client` to complete the model training and prediction process. One worker may contain one or more tasks, where as a task is a computing unit in Angel. In this way, tasks are able to share public resources of a worker.

3. **Model Layer**: is an abstract, virtue layer without physical components, which hosts functionalities such as model pull/push operations, multiple sync protocols, model partitioner, psFunc etc. This layer bridges between the worker layer and the PS layer.

In addition to the three modules, there are two important classes that deserve attention, though not shown in the chart:

1. **Client**: The Initiator of Angel Application

	* Start/stop PSServer
	* Start/stop Angel worker
	* Load/save Model
	* Start the specific computing process
	* Obtain application status


2. **Master**: The Guardian of Angel Application

	* Slice and distribute raw data and the parameter matrix
	* Request computing resources for worker and parameter server from Yarn
	* Coordinate, manage and monitor worker and PSServer activities

With the above design, Angel's overall architecture is comparatively easy to scale up.

* **PSServer Layer**: provide flexible and multi-framework PS support through PS-Service
* **Model Layer**: provide necessary functions of PS and support targeted optimization for performance
* **Worker Layer**: satisfy needs for algorithm development and innovation based on Angel's independent API

Therefore, with Angel，distributed computing engineers can focus on optimization of the core layers, whereas algorithm engineers and data scientists can focus on algorithm development and implementation, making full use of the platform, to pursue best performance and accuracy.


[1]: ../img/angel_architecture_1.png

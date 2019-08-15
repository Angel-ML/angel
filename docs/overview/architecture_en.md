# Angel's Architecture Design

----
## Angel  Architecture
![][1]

The overall design of Angel is simple, clear-cut, easy-to-use, without excessive complex design. It focuses on characteristics related to machine learning and models,  pursuing best performance of high-dimensional models. 

In general, Angel's architecture design consists of four type modules:

* Client: Submit job to the cluster
* Master:  Start and monitor PSs, Workers
* PS: Storage all the parameters, execute PSFs
* Worker: Training the model using data splits, Interact with PSs to update model and pull new model

Workers and PS can have multiple instances, so Angel can handle large data volumes and very high dimensional models.

[1]: ../img/angel_architecture_2.png

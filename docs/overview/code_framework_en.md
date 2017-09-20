# Angel's Code Framework

---

Overall, Angel's code framework consists of the following modules:

![][1]


  [1]: ../img/code_framework.png
  

## 1. Angle-Core (Core Layer)

Angel's core layer consists of the following main components:

* PSServer
* PSAgent
* Worker
* AngelClient
* Network: RPC & RDMA
* Storage: memory & Disk
* ……



## 2. Angel-ML (Machine-Learning Layer)

Angel is oriented toward machine learning, therefore, all the machine-learning related elements are added to the core, in a separate directory, including:

* Matrix
* Vector
* Feature
* Optimizer
* Objective
* Metric
* psFunc

Notice that the ML layer in the core package contains mainly low-level, basic interfaces, whereas their extentions and implementations belong to the specific algorithm layer (MLLib, to be introduced below). 

## 3. Angel-Client (Interface Layer)

Angel's interface layer supports plug-in extensions based on the bottom layer. Currently, there are two sets of API, one for Angel and the other for Spark. 

* **Angel API**

	The API for Angel itself is designed to focus on PSModel. The overall task execution reflects the simplicity and directness principles in the first-generation MR. MLRunner calls Learner through Task driver, and learns the MLModel that contains one or more PSModel. 


* **Spark on Angel API**

	Spark on Angel API focuses on Spark's integration and coordination on the Angel platform. The design of Angel's PS-Service makes it easy to develop Spark on Angel; in Spark, one can use Spark on Angel directly, with no need to change anything in the core. 

## 4. Angle-MLLib (Algorithm Layer)

We developed a variety of algorithms based on Angel interfaces:

* SVM
* LR (with multiple optimization methods)
* KMeans
* GBDT
* LDA
* MF (Matrix Factorization)
* FM (Factorization Machines)

Angel's algorithm development takes a straight-forward approach that centers around models on the PS, constantly updating them. Algorithm implementations are comparatively flexible, aiming for the best performance.

Overall, Angel users can pursue a full range of modular designs by programming at different layers, in order to develop efficient machine-learning algorithms. 


# Spark on Angel Optimizer

## 1. Overview
Data, features and numerical optimization algorithms are central to machine learning. Commmon numerical optimization algorithms include gradient descent, Newton's method, as well as their variants. The numerical optimization processing for most of the algorithms in Spark MLlib are implemented with [Breeze](https://github.com/scalanlp/breeze), a numerical processing library for Scala. Breeze provides a rich set of optimization algorithms with easy-to-use interfaces. 

In order for Spark on Angel to support algorithms in Breeze, our project uses a trick called **transparent replacement**. In Breeze, DenseVector and SparseVector (abbreviated as BreezeVector) are supported data structures defined with NumericOps trait, an example being L-BFGS's usage of BreezeVector's operations, such as dot, scal, etc. 

We have implemented NumericOps trait in PSVector for it to be operable by Breeze; by doing so, we have seamlessly transferred the optimization process of Breeze's algorithms, such as SGD and L-BFGS, to run on Angel. 

## 2. Logistic Regression
Logistic Regression is a commonly used machine learning classifier. The objective can be solved by different optimization methods, such as SGD, L-BFGS, OWL-QN, etc. In fact, these different optimization algorithms share a similar iteration process in distributed system: calculating the gradient using training data and updating the weights using the gradient. In Spark, the executors compute the gradients and losses in a distributed fashion, and merge into the driver via treeAggregate, where the model parameters are updated. Similarly, in Spark on Angel, executors compute the gradients and losses in parallel and increment the gradients to Angel PS, where the model parameters are updated.

In Spark MLlib, there is a significant amount of data pre-processing for Logistic Regression, and the algorithm is more suitable for scenarios with a low-dimensional, "dense" feature space. For Spark on Angel, we implemented Logistic Regression to suit high-dimensional, sparse feature spaces, while supporting SGD, L-BFGS and OWL-QN as the optimization options. 

## 3. Spark VS. Spark on Angel

We run experiments to compare Spark and Spark on Angel, with the Logistic Regression on latter implemented with three different optimization algorithms.

- Data: Tencent's internal data, 230M samples，50M features
- Experimental Settings: 
  + Note 1: we allocated more resources than actual needs to guarantee that all tasks run with sufficient resources   
  + Note 2: We had to increase spark.driver.maxResultSize in Spark


|  SGD LR   |     Executor                     |  Driver   |  Angel PS   |
|---|---|---|---|
| Spark | 100 Executors(14G RAM, 2 core)   | 50G        | -   |
| Spark on Angel  |  100 Executors(14G RAM, 2 core)  | 5G | 20 PS(5G RAM, 2core)  |

|  L-BFGS LR   |     Executor                     |  Driver   |  Angel PS   |
|---|---|---|---|
| Spark | 100 Executors(14G RAM, 2 core)   | 50G        | -   |
| Spark on Angel  |  100 Executors(14G RAM, 2 core)  | 5G | 20 PS(10G RAM, 2core)  |

|  OWL-QN LR   |     Executor                     |  Driver   |  Angel PS   |
|---|---|---|---|
| Spark | 100 Executors(14G RAM, 2 core)   | 50G        | -   |
| Spark on Angel  |  100 Executors(14G RAM, 2 core)  | 5G | 50 PS(10G RAM, 2core)  |

- Experiment Result

| item   |  Spark  |  Spark on Angel  |  Spark on Angel's Improvement in Speed   |
|---|---|---|---|
|SGD LR (step_size=0.05,maxIter=100) | 2.9 hour   | 2.1 hour   | 27.6%  |
|L-BFGS LR (m=10, maxIter=50)        | 2 hour     | 1.3 hour   | 35.0%  |
|OWL-QN LR (m=10, maxIter=50)        | 3.3 hour   | 1.9 hour   | 42.4%  |

As shown in the result above, Spark on Angel has improved speed for training of LR when compared to Spark. In general, the more complex the model, the more Spark on Angel can speed up training. It is worth noting that the algorithm's implementations in Spark and Spark on Angel have minimal differences, making the transition easier for Spark users.

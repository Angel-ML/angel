# Change Log

All notable changes to this project will be documented in this file.

This Change Log format is suggested by
<https://github.com/olivierlacan/keep-a-changelog/blob/master/CHANGELOG.md>

## Release-2.2.0 - 2019-05-06

In this Release, we have enhanced the graph algorithms: (1) we made a refactoring of the existing K-Core algorithm, the performance and stability have been significantly improved; (2) we add the louvain algorithm, which is also named Fast-Unfolding. The test results show that the K-Core and the Louvain algorithm are both 10x faster than GraphX. In this release we official release the Vero, a new GBDT implementation over Spark On Angel. The advantage of Vero is that it obtains great The main feature of Vero is which has obvious advantages in supporting high dimensional models and multi-classification problems. We also add kerberos support in this release.

New features in Release-2.2.0:

- Add Fast Unfolding algorithm in Spark-on-Angel
- Support predict for FTRL-LR in Spark-on-Angel
- Support predict for FTRL-FM in Spark-on-Angel
- Add Vero, a feature parallelism version GBDT on Spark-on-Angel
- Support regression for GBDT on Spark-on-Angel
- Add a new data split input format--BalanceInputFormatV2
- Support running over Kubernetes.

Bugs fixed in Release-2.2.0:

- Fix the failure of loading model when the model is moved, closing the csc check
- Fix the problem that parameter servers exist with errors in Spark-on-Angel
- Fix the problem that the interface of sparse index pull might be blocked when given parameters are invalid
- Fix the problem that saving result would fail if the parent path is not existed
- Fix the problem that the BalanceInputFormat would return empty splits sometimes
- Fix the problem when saving json configuration files
- Fix the problem when requesting the resources for Angel workers

## Release-2.1.0 - 2019-03-08

In this release, we add an intelligent model partitioning method, named"LoadBalancePartitioner", In Spark-on-Angel. By analyzing the distribution of features in the training data in advance, the number of features of each partition can be precisely controlled. This leads a balanced load for each server. The empirical tests demonstrate that the efficiency of model training can be greatly improved in many cases. Further, we add three algorithms in this release, including FM solved by FTRL optimizer, K-Core algorithm and feature-parallel GBDT, which can support a high-dimentional tree model.

New features in Release-2.1.0:

- Adding a load-balanced model partitioner, called "LoadBalancePartitioner" in Spark-on-Angel
- Adding Ftrl-FM algorithm
- Adding K-core algorithm
- Adding a feature-parallel version of GBDT algorithm

## Release-2.0.2 - 2019-01-30

In this release, we optimize the performance of FTRL algorithm, adding the support for float data type. We limit the maxmimum retry times for remote requests to avoid unrecoverable blocking. We also increase the performance of the math library.

New features in Release-2.0.2

- Optimize the model partitioning for FTRL algorithm
- Support float data type for FTRL algorithm
- Avoid rehashing in math library to obtain performance improvement
- Add a maxmimum retry times for remote requests on servers

## Release-2.0.1 - 2019-01-11

In this release, we add the support for incremental training for FTRL. We implement some new optimizers and learning rate scheduling strategies.
Documentations about how to choose optimizers or scheduling strategies and how to accelerate deep learning algorithms with openblas are provided 
in this release.

New features in Release-2.0.1

- Add documentations about how to use openblas to accelerate deep learning algorithms
- Optimize the performance for FTRL
- Support inremental training for FTRL
- Add optimizers with L1 penalty: Adagrad/Adadelta
- Add some scheduling strategies for learning rate

Bug Fix:

- Fix the problem of inconsistency of nodes number in network embedding
- Fix casting problem existing in quantile compressing

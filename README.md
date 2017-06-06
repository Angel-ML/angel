![](assets/angel_logo.png)


[![license](http://img.shields.io/badge/license-BSD3-brightgreen.svg?style=flat)](https://github.com/tencent/angel/blob/master/LICENSE)
[![Release Version](https://img.shields.io/badge/release-1.0.0-red.svg)](https://github.com/tencent/angel/releases) 
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](https://github.com/tencent/angel/pulls)


**Angel**是一个基于参数服务器（Parameter Server）理念的机器学习框架，支持高维度的分布式机器学习。

Angel的设计围绕机器学习模型，它将高维度的大模型，合理切分到多个参数服务器节点，并提供高效的模型更新操作接口和运算符，并在其之上高效实现各种机器学习算法。

Angel由Java和Scala语言开发，基于Yarn进行调度。 它既可以独立运行，也可以作为PS服务，支持Spark，以及其它深度学习框架，具有较好的灵活性。

Angel经过长达2年的开发，基于腾讯内部的海量数据进行了反复的实践和调优，并具有广泛的适用性和稳定性，可以用于多种机器学习框架的加速，应用于各种需要大模型的场合。它的性能在高维度模型下有很强的优势，无论是对Spark还是xgBoost。


## Angel的架构

* [Angel的整体架构](./docs/design/architecture.md)
* [Angel的设计理念](./docs/design/design.md)

## Angel入门
* [Quick Start]()
* [Spark on Angel入门](./docs/tutorials/spark_on_angel_quick_start.md)


## Angel的编程指南

* [Angel编程手册](./docs/programmers_guide/angel_programing_guide.md)
* [Spark on Angel编程手册](./docs/programmers_guide/spark_on_angel_programing_guide.md)

## Angel的接口

* [Angel的接口说明](./docs/apis/interface_api.md)
* [psFunc手册](./docs/design/pof_develop.md)

## Angel的算法

* [Logistic Regression](./docs/algo/lr_on_angel.md)
* [Matrix Factorization](./docs/algo/mf_on_angel.md)
* [SVM](./docs/algo/svm_on_angel.md)
* [KMeans](./docs/algo/kmeans_on_angel.md)
* [GBDT](./docs/algo/gbdt_on_angel.md)
* [LDA](./docs/algo/lda_on_angel.md)

## Angel的部署

* [源码下载和编译](./docs/deploy/source_compile.md)
* [本地运行](./docs/deploy/local_run.md)
* [Yarn运行](./docs/deploy/run_on_yarn.md)
* [主要系统配置](./docs/deploy/config_details.md)


## Spark on Angel

* [Spark on Angel的设计](./docs/design/spark_on_angel.md)

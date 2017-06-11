![](assets/angel_logo.png)


[![license](http://img.shields.io/badge/license-BSD3-brightgreen.svg?style=flat)](https://github.com/tencent/angel/blob/master/LICENSE)
[![Release Version](https://img.shields.io/badge/release-1.0.0-red.svg)](https://github.com/tencent/angel/releases) 
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](https://github.com/tencent/angel/pulls)


**Angel**是一个基于参数服务器（Parameter Server）理念的机器学习框架，它能让机器学习算法轻松运行于高维度模型之上。

**Angel**的核心设计理念围绕**模型**。它将高维度的大模型，合理切分到多个参数服务器节点，并通过高效的`模型更新接口`和`运算函数`，以及多变的`同步协议`，轻松实现各种高效的机器学习算法。

**Angel**由**Java**和**Scala**开发，基于**Yarn**调度运行，既能独立运行，高效运行特有的算法，亦能作为PS Service，支持**Spark**或其它深度学习框架，为其加速。它基于腾讯内部的海量数据进行了反复的实践和调优，并具有广泛的适用性和稳定性，模型维度越高，优势越明显。

欢迎对机器学习有兴趣的同仁一起贡献代码，提交Issues或者Pull Requests。请先查阅: [Angel Contribution Guide]()

## Angel的架构

* [Angel的整体架构](./docs/design/architecture.md)
* [Angel的设计理念](./docs/design/design.md)

## Angel入门
* [Quick Start](./docs/tutorials/angel_ps_quick_start.md/)
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

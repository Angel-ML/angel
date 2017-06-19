![](assets/angel_logo.png)


[![license](http://img.shields.io/badge/license-BSD3-brightgreen.svg?style=flat)](https://github.com/tencent/angel/blob/master/LICENSE)
[![Release Version](https://img.shields.io/badge/release-1.0.0-red.svg)](https://github.com/tencent/angel/releases)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](https://github.com/tencent/angel/pulls)


**Angel**是一个基于参数服务器（Parameter Server）理念开发的高性能分布式机器学习平台，它基于腾讯内部的海量数据进行了反复的调优，并具有广泛的适用性和稳定性，模型维度越高，优势越明显。 **Angel**由腾讯和北京大学联合开发，兼顾了工业界的高可用性和学术界的创新性。

**Angel**的核心设计理念围绕**模型**。它将高维度的大模型合理切分到多个参数服务器节点，并通过高效的**模型更新接口和运算函数**，以及灵活的**同步协议**，轻松实现各种高效的机器学习算法。

**Angel**基于**Java**和**Scala**开发，能在社区的**Yarn**上直接调度运行，并基于**PS Service**，支持**Spark on Angel**，未来将会支持图计算和深度学习框架集成。

欢迎对机器学习有兴趣的同仁一起贡献代码，提交Issues或者Pull Requests。请先查阅: [Angel Contribution Guide](https://github.com/Tencent/angel/blob/master/CONTRIBUTING.md)

## Angel的介绍

* [架构设计](./docs/overview/architecture.md)
* [系统框架](./docs/overview/framework.md)
* [设计理念](./docs/overview/design.md)
* [Spark on Angel的设计](./docs/overview/spark_on_angel.md)


## Angel入门
* [Quick Start](./docs/tutorials/angel_ps_quick_start.md/)
* [Spark on Angel入门](./docs/tutorials/spark_on_angel_quick_start.md)


## Programming Guide

* [Angel编程手册](./docs/programmers_guide/angel_programing_guide.md)
* [Spark on Angel编程手册](./docs/programmers_guide/spark_on_angel_programing_guide.md)

## Design

* [核心类的说明](./docs/apis/interface_api.md)
* [psFunc手册](./docs/design/psf_develop.md)

## Algorithm

* [Logistic Regression](./docs/algo/lr_on_angel.md)
* [Matrix Factorization](./docs/algo/mf_on_angel.md)
* [SVM](./docs/algo/svm_on_angel.md)
* [KMeans](./docs/algo/kmeans_on_angel.md)
* [GBDT](./docs/algo/gbdt_on_angel.md)
* [LDA](./docs/algo/lda_on_angel.md)
* [Spark on Angel Optimizer](./docs/algo/spark_on_angel_optimizer.md)

## Deployment

* [源码下载和编译](./docs/deploy/source_compile.md)
* [本地运行](./docs/deploy/local_run.md)
* [Yarn运行](./docs/deploy/run_on_yarn.md)
* [系统配置](./docs/deploy/config_details.md)

## Papers
  1. Jiawei Jiang, Bin Cui, Ce Zhang, Lele Yu
  
  	 [Heterogeneity-aware Distributed Parameter Servers](http://net.pku.edu.cn/~cuibin/Papers/2017%20sigmod.pdf).   SIGMOD, 2017
  2. Jie Jiang, Lele Yu, Jiawei Jiang, Yuhong Liu and Bin Cui
  
     [Angel: a new large-scale machine learning system](http://net.pku.edu.cn/~cuibin/Papers/2017NSRangel.pdf). National Science Review (NSR), 2017




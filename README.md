![](assets/angel_logo.png)


[![license](http://img.shields.io/badge/license-Apache2.0-blue.svg?style=flat)](https://github.com/tencent/angel/blob/master/LICENSE)
[![Release Version](https://img.shields.io/badge/release-2.0.0-red.svg)](https://github.com/tencent/angel/releases)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](https://github.com/tencent/angel/pulls)

[(English Documents Available)](./README_en.md)

**Angel**是一个基于参数服务器（Parameter Server）理念开发的高性能分布式机器学习平台，它基于腾讯内部的海量数据进行了反复的调优，并具有广泛的适用性和稳定性，模型维度越高，优势越明显。 **Angel**由腾讯和北京大学联合开发，兼顾了工业界的高可用性和学术界的创新性。

**Angel**的核心设计理念围绕**模型**。它将高维度的大模型合理切分到多个参数服务器节点，并通过高效的**模型更新接口和运算函数**，以及灵活的**同步协议**，轻松实现各种高效的机器学习算法。

**Angel**基于**Java**和**Scala**开发，能在社区的**Yarn**上直接调度运行，并基于**PS Service**，支持**Spark on Angel**，集成了部分图计算和深度学习算法。

欢迎对机器学习有兴趣的同仁一起贡献代码，提交Issues或者Pull Requests。请先查阅: [Angel Contribution Guide](https://github.com/Tencent/angel/blob/master/CONTRIBUTING.md)

## Overview

* [架构设计](./docs/overview/architecture.md)
* [代码结构](./docs/overview/code_framework.md)
* [设计理念](./docs/overview/design_philosophy.md)
* [Spark on Angel](./docs/overview/spark_on_angel.md)


## Design
* [模型格式](./docs/design/model_format.md)
* [模型切分（modelPartitioner）](./docs/design/model_partitioner.md)
* [异步控制（syncController）](./docs/design/sync_controller.md)
* [定制函数（psFunc）](./docs/design/psfFunc.md)
* [核心接口](./docs/apis/core_api.md)
* [周边辅助](./docs/assistant/hobby_api.md)

## Programming Guide

* [Angel编程手册](./docs/programmers_guide/angel_programing_guide.md)
* [Spark on Angel编程手册](./docs/programmers_guide/spark_on_angel_programing_guide.md)

## Deep Learning Architexture
* [Angel中的计算图](./docs/basic/computinggraph_on_angel.md)
* [Angel中的层](./docs/basic/layers_on_angel.md)
* [Angel中优化器](./docs/basic/optimizer_on_angel.md)
* [Angel中的损失函数](./docs/basic/lossfunc_on_angel.md)
* [Angel中的传输函数](./docs/basic/transfunc_on_angel.md)
* [Angel中的学习率Decay](./docs/basic/batchsize_decay.md)

## Quick Start
* [Angel入门](./docs/tutorials/angel_ps_quick_start.md)
* [Spark on Angel入门](./docs/tutorials/spark_on_angel_quick_start.md)
* [Angel Json配置](./docs/basic/json_conf.md)

## Algorithm
* [**Angel or Spark On Angel？**](./docs/algo/angel_or_spark_on_angel.md)
* [**Algorithm Parameter Description**](./docs/algo/model_config_details.md)
* **Angel**
	* **Traditional Machine Learning Methods**
		* [Logistic Regression(LR)](./docs/algo/lr_on_angel.md)
		* [Support Vector Machine(SVM)](./docs/algo/svm_on_angel.md)
		* [Factorization Machine(FM)](./docs/algo/fm_on_angel.md)
		* [Linear Regression](./docs/algo/linear_on_angel.md)
		* [Robust Regression](./docs/algo/robust_on_angel.md)
		* [Softmax Regression](./docs/algo/softmax_on_angel.md)
		* [KMeans](./docs/algo/kmeans_on_angel.md)
		* [GBDT](./docs/algo/gbdt_on_angel.md)
		* [LDA\*](./docs/algo/lda_on_angel.md) ([WrapLDA](./docs/algo/wrap_lda_on_angel.md))
	* **Deep Learning Methods**
		* [Deep Neural Network(DNN)](./docs/algo/dnn_on_angel.md)
		* [Mix Logistic Regression(MLR)](./docs/algo/mlr_on_angel.md)
		* [Deep And Wide(DAW)](./docs/algo/daw_on_angel.md)
		* [Deep Factorization Machine(DeepFM)](./docs/algo/deepfm_on_angel.md)
		* [Neural Factorization Machine(NFM)](./docs/algo/nfm_on_angel.md)
		* [Product Neural Network(PNN)](./docs/algo/pnn_on_angel.md)
* **Spark on Angel**
	* [FTRL](./docs/algo/ftrl_lr_spark.md)
	* [Logistic Regression(LR)](./docs/algo/sona/lr_sona.md)
	* [Word2Vec](./docs/algo/sona/word2vec_sona.md)
	* [LINE](./docs/algo/sona/line_sona.md)


## Deployment

* [下载和编译](./docs/deploy/source_compile.md)
* [本地运行](./docs/deploy/local_run.md)
* [Yarn运行](./docs/deploy/run_on_yarn.md)
* [系统配置](./docs/deploy/config_details.md)
* [资源配置指南](./docs/deploy/resource_config_guide.md)
* [使用OpenBlas给算法加速](./docs/deploy/blas_for_densematrix.md)

## FAQ
* [工程类问题](https://github.com/Tencent/angel/wiki/%E5%B7%A5%E7%A8%8B%E5%B8%B8%E8%A7%81%E9%97%AE%E9%A2%98)
* [算法类问题](https://github.com/Tencent/angel/wiki/%E7%AE%97%E6%B3%95%E5%B8%B8%E8%A7%81%E9%97%AE%E9%A2%98)

## Support

* **QQ群**：20171688

## Papers
  1. Lele Yu, Bin Cui, Ce Zhang, Yingxia Shao. [LDA*: A Robust and Large-scale Topic Modeling System](http://www.vldb.org/pvldb/vol10/p1406-yu.pdf). VLDB, 2017
  2. Jiawei Jiang, Bin Cui, Ce Zhang, Lele Yu. [Heterogeneity-aware Distributed Parameter Servers](http://dl.acm.org/citation.cfm?id=3035933). SIGMOD, 2017
  3. Jie Jiang, Lele Yu, Jiawei Jiang, Yuhong Liu and Bin Cui. [Angel: a new large-scale machine learning system](https://academic.oup.com/nsr/article/3052720). National Science Review (NSR), 2017
  4. Jie Jiang, Jiawei Jiang,  Bin Cui and Ce Zhang. [TencentBoost: A Gradient Boosting Tree System with Parameter Server](http://ieeexplore.ieee.org/abstract/document/7929984/).	ICDE, 2017
  5. Jiawei Jiang, Bin Cui, Ce Zhang and Fangcheng Fu. [DimBoost: Boosting Gradient Boosting Decision Tree to Higher Dimensions](https://dl.acm.org/citation.cfm?id=3196892). SIGMOD, 2018.

## Presentation

1. [Angel: A Machine Learning Framework for High Dimensionality](https://cdn.oreillystatic.com/en/assets/1/event/273/Angel_%E9%9D%A2%E5%90%91%E9%AB%98%E7%BB%B4%E5%BA%A6%E7%9A%84%E6%9C%BA%E5%99%A8%E5%AD%A6%E4%B9%A0%E8%AE%A1%E7%AE%97%E6%A1%86%E6%9E%B6%20_Angel_%20A%20machine%20learning%20framework%20for%20high%20dimensionality_%20%E8%AE%B2%E8%AF%9D.pdf).  Strata China, 2017

2. [方圆并济：基于 Spark on Angel 的高性能机器学习](./docs/slides/Angel_QCon_2017.pdf).  QCon ShangHai China, 2017

3. [基于Angel和Spark Streaming的高维度Online Learning](./docs/slides/Angel_GIAC_2017.pdf). GIAC China, 2017

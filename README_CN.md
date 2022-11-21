![](assets/angel_logo.png)

[![license](http://img.shields.io/badge/license-Apache2.0-brightgreen.svg?style=flat)](https://github.com/Angel-ML/angel/blob/branch-3.2.0/LICENSE.TXT)
[![Release Version](https://img.shields.io/badge/release-3.2.0-red.svg)](https://github.com/tencent/angel/releases)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](https://github.com/tencent/angel/pulls)
[![Download Code](https://img.shields.io/badge/download-zip-green.svg)](https://github.com/Angel-ML/angel/archive/refs/heads/branch-3.2.0.zip)

[(English Documents Available)](./README_en.md)

**Angel**是一个基于参数服务器（Parameter Server）理念开发的高性能分布式机器学习和图计算平台，它基于腾讯内部的海量数据进行了反复的调优，并具有广泛的适用性和稳定性，模型维度越高，优势越明显。 **Angel**由腾讯和北京大学联合开发，兼顾了工业界的高可用性和学术界的创新性。

**Angel**的核心设计理念围绕**模型**。它将高维度的大模型合理切分到多个参数服务器节点，并通过高效的**模型更新接口和运算函数**，以及灵活的**同步协议**，轻松实现各种高效的机器学习和图算法。

**Angel**基于**Java**和**Scala**开发，能在社区的**Yarn**上直接调度运行，并基于**PS Service**，支持**Spark on Angel**，集成了图计算和深度学习算法。

欢迎对机器学习、图计算有兴趣的同仁一起贡献代码，提交Issues或者Pull Requests。请先查阅: [Angel Contribution Guide](https://github.com/Tencent/angel/blob/master/CONTRIBUTING.md)

## Overview

* [架构设计](./docs/overview/architecture.md)
* [代码结构](./docs/overview/code_framework.md)
* [设计理念](./docs/overview/design_philosophy.md)
* [Spark on Angel](./docs/overview/spark_on_angel.md)
  * [机器学习](./docs/overview/spark_on_angel.md)
  * [图计算](./docs/overview/angel_graph_sona.md)


## Design
* [模型格式](./docs/design/model_format.md)
* [模型切分（modelPartitioner）](./docs/design/model_partitioner.md)
* [异步控制（syncController）](./docs/design/sync_controller.md)
* [定制函数（psFunc）](./docs/design/psfFunc.md)
* [核心接口](./docs/apis/core_api.md)
* [周边辅助](./docs/assistant/hobby_api.md)

## Quick Start
* [Spark on Angel入门](./docs/tutorials/spark_on_angel_quick_start.md)

## Deployment

* [下载和编译](./docs/deploy/source_compile.md)
* [本地运行](./docs/deploy/local_run.md)
* [Yarn运行](./docs/deploy/run_on_yarn.md)
* [系统配置](./docs/deploy/config_details.md)
* [资源配置指南](./docs/deploy/resource_config_guide.md)
* [使用OpenBlas给算法加速](./docs/deploy/blas_for_densematrix.md)

## Programming Guide

* [Angel编程手册](./docs/programmers_guide/angel_programing_guide.md)
* [Spark on Angel编程手册](./docs/programmers_guide/spark_on_angel_programing_guide.md)

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
		* [LDA\*](./docs/algo/lda_on_angel.md) ([WarpLDA](./docs/algo/warp_lda_on_angel.md))
* **Spark on Angel**
	* **Angel Mllib**
		* [FM](https://github.com/Angel-ML/PyTorch-On-Angel/blob/branch-0.2.0/docs/recommendation.md)
		* [DeepFM](https://github.com/Angel-ML/PyTorch-On-Angel/blob/branch-0.2.0/docs/recommendation.md)
		* [DeepAndWide](https://github.com/Angel-ML/PyTorch-On-Angel/blob/branch-0.2.0/docs/recommendation.md)
		* [DCN](https://github.com/Angel-ML/PyTorch-On-Angel/blob/branch-0.2.0/docs/recommendation.md)
		* [XDeepFM](https://github.com/Angel-ML/PyTorch-On-Angel/blob/branch-0.2.0/docs/recommendation.md)
		* [AttentionFM](https://github.com/Angel-ML/PyTorch-On-Angel/blob/branch-0.2.0/docs/recommendation.md)
  	    * [PNN](https://github.com/Angel-ML/PyTorch-On-Angel/blob/branch-0.2.0/docs/recommendation.md)
        * [FTRL](./docs/algo/ftrl_lr_spark.md)
        * [Logistic Regression(LR)](./docs/algo/sona/lr_sona.md)
        * [FTRLFM](./docs/algo/ftrl_fm_spark_en.md)
        * [GBDT](./docs/algo/sona/feature_gbdt_sona.md)
  * **Angel Graph**
    * [PageRank](./docs/algo/sona/pagerank_on_sona.md)
    * [KCORE](./docs/algo/sona/kcore_sona.md)
    * [HIndex](./docs/algo/sona/hindex_sona.md)
    * [Closeness](./docs/algo/sona/closeness_sona.md)
    * [CommonFriends](./docs/algo/sona/commonfriends_sona.md)
    * [ConnectedComponents](./docs/algo/sona/CC_sona.md)
    * [TriangleCountingUndirected](./docs/algo/sona/triangle_count_undirected.md)
    * [Louvain](./docs/algo/sona/louvain_sona.md)
    * [LPA](./docs/algo/sona/LPA_sona.md)
    * [LINE](./docs/algo/sona/line_sona.md)
    * [Word2Vec](./docs/algo/sona/word2vec_sona.md)
    * [GraphSage](https://github.com/Angel-ML/PyTorch-On-Angel/blob/branch-0.2.0/docs/graph.md)
    * [GCN](https://github.com/Angel-ML/PyTorch-On-Angel/blob/branch-0.2.0/docs/graph.md)
    * [DGI](https://github.com/Angel-ML/PyTorch-On-Angel/blob/branch-0.2.0/docs/graph.md)

## Community
* Mailing list: angel-tsc@lists.deeplearningfoundation.org
* Angel homepage in Linux FD: https://angelml.ai/
* [Committers & Contributors](./COMMITTERS.md)
* [Contributing to Angel](./CONTRIBUTING.md)
* [Roadmap](https://github.com/Angel-ML/angel/wiki/Roadmap)

## FAQ
* [工程类问题](https://github.com/Tencent/angel/wiki/%E5%B7%A5%E7%A8%8B%E5%B8%B8%E8%A7%81%E9%97%AE%E9%A2%98)
* [算法类问题](https://github.com/Tencent/angel/wiki/%E7%AE%97%E6%B3%95%E5%B8%B8%E8%A7%81%E9%97%AE%E9%A2%98)

## Support

* **QQ群**：20171688

* **微信答疑群**：（加微信小助手，备注Angel答疑）

  ![](/docs/img/wx_support.png )

## Papers
  1. [PaSca: A Graph Neural Architecture Search System under the Scalable Paradigm](https://dl.acm.org/doi/pdf/10.1145/3485447.3511986). WWW, 2022
  2. [Graph Attention Multi-Layer Perceptron](https://dl.acm.org/doi/pdf/10.1145/3534678.3539121). KDD, 2022
  3. [Node Dependent Local Smoothing for Scalable Graph Learning](https://proceedings.neurips.cc/paper/2021/file/a9eb812238f753132652ae09963a05e9-Paper.pdf). NeurlPS, 2021
  4. [PSGraph: How Tencent trains extremely large-scale graphs with Spark?](https://conferences.computer.org/icde/2020/pdfs/ICDE2020-5acyuqhpJ6L9P042wmjY1p/290300b549/290300b549.pdf).ICDE, 2020.
  5. [DimBoost: Boosting Gradient Boosting Decision Tree to Higher Dimensions](https://dl.acm.org/citation.cfm?id=3196892). SIGMOD, 2018.
  6. [LDA*: A Robust and Large-scale Topic Modeling System](http://www.vldb.org/pvldb/vol10/p1406-yu.pdf). VLDB, 2017
  7. [Heterogeneity-aware Distributed Parameter Servers](http://net.pku.edu.cn/~cuibin/Papers/2017%20sigmod.pdf). SIGMOD, 2017
  8. [Angel: a new large-scale machine learning system](http://net.pku.edu.cn/~cuibin/Papers/2017NSRangel.pdf). National Science Review (NSR), 2017
  9. [TencentBoost: A Gradient Boosting Tree System with Parameter Server](http://net.pku.edu.cn/~cuibin/Papers/2017%20ICDE%20boost.pdf).	ICDE, 2017
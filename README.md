![](assets/angel_logo.png)

[![license](http://img.shields.io/badge/license-Apache2.0-brightgreen.svg?style=flat)](https://github.com/Angel-ML/angel/blob/branch-3.2.0/LICENSE.TXT)
[![Release Version](https://img.shields.io/badge/release-3.2.0-red.svg)](https://github.com/tencent/angel/releases)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](https://github.com/tencent/angel/pulls)
[![Download Code](https://img.shields.io/badge/download-zip-green.svg)](https://github.com/Angel-ML/angel/archive/refs/heads/branch-3.2.0.zip)

[(ZH-CN Version)](./README_CN.md)

**Angel** is a high-performance distributed machine learning and graph computing platform based on the philosophy of Parameter Server. It is tuned for performance with big data from Tencent and has a wide range of applicability and stability, demonstrating increasing advantage in handling higher dimension model. Angel is jointly developed by Tencent and Peking University, taking account of both high availability  in industry and innovation in academia.

With model-centered core design concept, **Angel** partitions parameters of complex models into multiple parameter-server nodes, and implements a variety of machine learning algorithms and graph algorithms using efficient model-updating interfaces and functions, as well as flexible consistency model for synchronization.

**Angel** is developed with **Java** and **Scala**.  It supports running on **Yarn**. With **PS Service** abstraction, it supports **Spark on Angel**.  Graph computing and deep learning frameworks support is under development and will be released in the future.

We welcome everyone interested in machine learning or graph computing to contribute code, create issues or pull requests. Please refer to  [Angel Contribution Guide](https://github.com/Tencent/angel/blob/master/CONTRIBUTING.md) for more detail.

## Introduction to Angel

* [Architecture](./docs/overview/architecture_en.md)
* [Code Framework](./docs/overview/code_framework_en.md)
* [Design](./docs/overview/design_philosophy_en.md)
* [Spark on Angel](./docs/overview/spark_on_angel_en.md)
  * [Machine Learning](./docs/overview/spark_on_angel_en.md)
  * [Graph Computing](./docs/overview/angel_graph_sona_en.md)

## Design

- [Model Partitioner](./docs/design/model_partitioner_en.md)
- [SyncController](./docs/design/sync_controller_en.md)
- [psFunc](./docs/design/psfFunc_en.md)
- [Core API](./docs/apis/core_api_en.md)


## Quick Start

* [Quick Start Example](./docs/tutorials/spark_on_angel_quick_start_en.md)

## Deployment

* [Compilation Guide](./docs/deploy/source_compile_en.md)
* [Running on Local](./docs/deploy/local_run_en.md)
* [Running on Yarn](./docs/deploy/run_on_yarn_en.md)
* [Configuration Details](./docs/deploy/config_details_en.md)
* [Resource Configuration Guide](./docs/deploy/resource_config_guide_en.md)

## Programming Guide

* [Spark on Angel Programming Guide](./docs/programmers_guide/spark_on_angel_programing_guide_en.md)

## Algorithm

- [**Angel or Spark On Angel？**](./docs/algo/angel_or_spark_on_angel.md)
- [**Algorithm Parameter Description**](./docs/algo/model_config_details.md)
- **Angel**
  - **Traditional Machine Learning Methods**
    - [Logistic Regression(LR)](./docs/algo/lr_on_angel_en.md)
    - [Support Vector Machine(SVM)](./docs/algo/svm_on_angel_en.md)
    - [Factorization Machine(FM)](./docs/algo/fm_on_angel.md)
    - [Linear Regression](./docs/algo/linear_on_angel_en.md)
    - [Robust Regression](./docs/algo/robust_on_angel_en.md)
    - [Softmax Regression](./docs/algo/softmax_on_angel_en.md)
    - [KMeans](./docs/algo/kmeans_on_angel_en.md)
    - [GBDT](./docs/algo/gbdt_on_angel_en.md)
    - [LDA\*](./docs/algo/lda_on_angel_en.md) ([WarpLDA](./docs/algo/warp_lda_on_angel.md))
- **Spark on Angel**
  - **Angel Mllib**
    - [FM](https://github.com/Angel-ML/PyTorch-On-Angel/blob/branch-0.2.0/docs/recommendation.md)
    - [DeepFM](https://github.com/Angel-ML/PyTorch-On-Angel/blob/branch-0.2.0/docs/recommendation.md)
    - [DeepAndWide](https://github.com/Angel-ML/PyTorch-On-Angel/blob/branch-0.2.0/docs/recommendation.md)
    - [DCN](https://github.com/Angel-ML/PyTorch-On-Angel/blob/branch-0.2.0/docs/recommendation.md)
    - [XDeepFM](https://github.com/Angel-ML/PyTorch-On-Angel/blob/branch-0.2.0/docs/recommendation.md)
    - [AttentionFM](https://github.com/Angel-ML/PyTorch-On-Angel/blob/branch-0.2.0/docs/recommendation.md)
    - [PNN](https://github.com/Angel-ML/PyTorch-On-Angel/blob/branch-0.2.0/docs/recommendation.md)
    - [FTRL](./docs/algo/ftrl_lr_spark.md)
    - [Logistic Regression(LR)](./docs/algo/sona/lr_sona.md)
    - [FTRLFM](./docs/algo/ftrl_fm_spark_en.md)
    - [GBDT](./docs/algo/sona/feature_gbdt_sona.md)
  - **Angel Graph**
    - [PageRank](./docs/algo/sona/pagerank_on_sona_en.md)
    - [KCORE](./docs/algo/sona/kcore_sona_en.md)
    - [HIndex](./docs/algo/sona/hindex_sona_en.md)
    - [Closeness](./docs/algo/sona/closeness_sona_en.md)
    - [CommonFriends](./docs/algo/sona/commonfriends_sona_en.md)
    - [ConnectedComponents](./docs/algo/sona/CC_sona_en.md)
    - [TriangleCountingUndirected](./docs/algo/sona/triangle_count_undirected_en.md)
    - [Louvain](./docs/algo/sona/louvain_sona_en.md)
    - [LPA](./docs/algo/sona/LPA_sona_en.md)
    - [LINE](./docs/algo/sona/line_sona_en.md)
    - [Word2Vec](./docs/algo/sona/word2vec_sona_en.md)
    - [GraphSage](https://github.com/Angel-ML/PyTorch-On-Angel/blob/branch-0.2.0/docs/graph.md)
    - [GCN](https://github.com/Angel-ML/PyTorch-On-Angel/blob/branch-0.2.0/docs/graph.md)
    - [DGI](https://github.com/Angel-ML/PyTorch-On-Angel/blob/branch-0.2.0/docs/graph.md)

## Community
* Mailing list: angel-tsc@lists.deeplearningfoundation.org
* Angel homepage in Linux FD: https://angelml.ai/
* [Committers & Contributors](./COMMITTERS.md)
* [Contributing to Angel](./CONTRIBUTING.md)
* [Roadmap](https://github.com/Angel-ML/angel/wiki/Roadmap)

## FAQ
* [Angel FAQ](https://github.com/Tencent/angel/wiki/Angel%E5%B8%B8%E8%A7%81%E9%97%AE%E9%A2%98)

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

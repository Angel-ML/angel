# Angel的代码结构

---

![][1]

Angel的代码主体由Java和Scala编写而成，依据一定的最佳实践，原则是让代码有最大限度的可复用和可扩展性，代码结构如下：

## 核心层（Core）

## 接口层（Client）

## 机器学习优化（ML-Optimization）

## 算法层（ML)

算法层是具体的算法实现，基于Angel的目前主要有6种算法，包括了：**SVM，LR，KMeans，GBDT，LDA，MF**
基于Spark on Angel的，目前主要有**ADMM-LR**算法。



  [1]: ../img/angel_framework.png
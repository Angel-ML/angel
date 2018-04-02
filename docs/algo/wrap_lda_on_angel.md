# WarpLDA

> WrapLDA是LDA的经典实现之一，WarpLDA中采用Metropolis Hastings (MH) 的方法进行话题采样。而LDA\*中采用F+LDA作为话题的采样器


## 1. 算法介绍

利用MH的方法能够将每次采样的操作降低到O(1)复杂度。但是由于MH采用了近似的方法，WarpLDA需要更多的采样操作，即更多的迭代次数才能收敛，这在分布式环境下意味着更多的网络通信开销。

感谢[@Chris19920210](https://github.com/Chris19920210) 的贡献

## 2. 运行 & 性能

### 运行命令

WarpLDA的运行方法和参数和LDA*相同

### 输入格式

I输入数据分为多行，每行是一个文档，每个文档由文档id和一系列的词id构成，文档id和词id之间由'\t'符合
相隔，词id之间由空格隔开

```
	doc_id '\t' wid_0 wid_1 ... wid_n 
```

### 参数

* 数据参数
  * angel.train.data.path: 输入数据路径
  * angel.save.model.path: 模型保存路径
* 算法参数
  * ml.epoch.num: 算法迭代次数
  * ml.lda.word.num：词个数
  * ml.lda.topic.num：话题个数
  * ml.worker.thread.num：worker内部并行度
  * ml.lda.alpha: alpha
  * ml.lda.beta: beta
  * save.doc.topic: 是否存储文档-话题矩阵
  * save.word.topic: 是否存储词-话题矩阵

## Reference

1. Lele Yu, Bin Cui, Ce Zhang, Yingxia Shao. [LDA*: A Robust and Large-scale Topic Modeling System](http://www.vldb.org/pvldb/vol10/p1406-yu.pdf). VLDB, 2017
2. Jianfei Chen, Kaiwei Li, Jun Zhu, Wenguan Chen. [WarpLDA: a Cache Efficient O(1) Algorithm for
Latent Dirichlet Allocation](http://www.vldb.org/pvldb/vol9/p744-chen.pdf)

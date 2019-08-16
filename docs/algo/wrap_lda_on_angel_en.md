# WarpLDA

> WrapLDA is one of the classical implementations of LDA. In WarpLDA, Metropolis Hastings (MH) is used to sample topics. In LDA*, F + LDA is used as topic sampler.  

## 1. Algorithm Introduction

The MH method can reduce the operation of each sample to O(1) complexity. However, since MH adopts an approximate method, WarpLDA requires more sampling operations, that is, more iterations can converge, which means more network communication overhead in a distributed environment.

Thanks to[@Chris19920210](https://github.com/Chris19920210) for his contribution.

## 2. Operation & performance

### Running commands

The operation method and parameters of WarpLDA are the same as LDA\*

### Input Format

The input data is divided into multiple lines, each line is a document, each document consists of a document id and a series of word ids, the document id and the word id are separated by a '\t' symbol, and the word id is separated by a space.

```
	doc_id '\t' wid_0 wid_1 ... wid_n 
```

### Parameter

* Data parameter
  * angel.train.data.path: input data path
  * angel.save.model.path: model save path
* Algorithm parameter
  * ml.epoch.num: iteration number of algorithms
  * ml.lda.word.num：number of words
  * ml.lda.topic.num：number of topics
  * ml.worker.thread.num：internal parallelism of worker
  * ml.lda.alpha: alpha
  * ml.lda.beta: beta
  * save.doc.topic: document-Topic matrix to be stored，whether or not
  * save.word.topic: word-topic matrix to be stored, whether or not

## Reference

1. Lele Yu, Bin Cui, Ce Zhang, Yingxia Shao. [LDA*: A Robust and Large-scale Topic Modeling System](http://www.vldb.org/pvldb/vol10/p1406-yu.pdf). VLDB, 2017
2. Jianfei Chen, Kaiwei Li, Jun Zhu, Wenguan Chen. [WarpLDA: a Cache Efficient O(1) Algorithm for
Latent Dirichlet Allocation](http://www.vldb.org/pvldb/vol9/p744-chen.pdf)

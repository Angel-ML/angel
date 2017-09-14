# 定制函数（psFunc）

---

作为标准的参数服务器，正常都会提供基本的参数 **获取（pull）** 和 **更新（push）** 功能。但实际应用中，各个算法对参数服务器上的参数获取和更新，却远远不只这么简单，尤其是当算法需要实施一些特定的优化的时候。

> 举个例子，有时候某些算法，要得到`矩阵模型`中某一行的最大值，如果PS系统，只有基本的Pull接口，那么PSClient，就只能先将该行的所有列，都从参数服务器上拉取回来，然后在Worker上计算得到最大值，这样会产生很多的网络通信开销，对性能会有影响。而如果我们有一个自定义函数，每个PSServer在远程先计算出n个局部最大值，再交换确认全局最大值，这时只要返回1个数值就可以了，这样的方式，计算开销接近，但通信开销将大大降低。


为了解决类似的问题，Angel引入和实现psFunc的概念，对远程模型的获取和更新的流程进行了封装和抽象。它也是一种用户自定义函数（UDF），但都和PS操作密切相关的，因此被成为**psFunc**，简称**psf**，整体架构如下：

![](../img/angel_psFunc.png)

随着psFunc的引入，模型的计算，也会发生在PSServer端。PSServer也将有一定的模型计算职责，而不是单纯的模型存储功能。合理的设计psFunc，将大大的加速算法的运行。

值得一提的是，伴随着psFunc的引入和强化，在很多复杂的算法实现中，降低了Worker要把模型完整的拖回来进行整体计算的可能性，从而间接地实现了**模型并行**。

## 分类

整体上，Angel的psFunc分成两大类，一类是获取型，一类是更新型

* [GetFunc(获取类函数)](psf_get.md)
* [UpdateFunc(更新类函数)](psf_update.md)

PSF从数据交互角度可以分成 **Worker-to-PSServer** 和 **PSServer-to-PSServer** 两种类型。
* Worker-to-PSServer是指Worker的数据和PSServer的数据做运算。如Push、Pull，将Worker本地的Vector Push给PS，或者将PS上的Row Pull到Worker上；

* PSServer-to-PSServer是指PSServer上的matrix内部row直接发生的运算。如Add、Copy等，Add是将matrix两行的数据相加，将结果保存在另外一行；Copy是将某行的内容拷贝到另外一行。

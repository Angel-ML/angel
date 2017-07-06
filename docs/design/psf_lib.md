为了方便用户，Angel提供了一套内置的PSF（psFunc），供用户调用和参考，可以在此基础上，写出更好的psf。

PSF整体可以分为两大类：

1. Client 2 PSServer
2. PSServer 2 PSServer

它们的区别在于

> 第一类的运算，是客户端触发Server的，而第二类的运算，发生于PSServer之间

* [聚合类函数](psf_aggregatelib.md)
* [更新函数](psf_updatelib.md)
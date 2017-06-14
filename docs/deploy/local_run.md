# 本地运行模式

---

## 1. 运行环境准备

* Hadoop >= 2.2.0
* Java 1.8版本
* Angel发布包 angel-1.0.0-bin.zip

配置好HADOOP_HOME和JAVA_HOME环境变量，解压Angel发布包，就可以以LOCAL模式运行Angel任务了。

## 2. LOCAL 运行例子

发布包解压后，在根目录下有一个bin目录，提交任务相关的脚本都放在该目录下。例如运行简单的逻辑回归的例子：

```./angel-example com.tencent.angel.example.SgdLRLocalExample```

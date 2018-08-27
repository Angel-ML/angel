# 本地运行模式

本地运行模式主要用于测试功能是否正确。目前本地运行模式仅支持一个Worker（可以有多个Task）和一个PS。可以通过配置选项`angel.deploy.mode`来使用本地运行模式，具体的参数配置可参考[Angel系统参数](./config_details.md)

## 1. 运行环境准备

* Java >= 1.8
* Angel发布包 angel-\<version\>-bin.zip

配置好HADOOP_HOME和JAVA_HOME环境变量，解压Angel发布包，就可以以LOCAL模式运行Angel任务了。

## 2. LOCAL 运行例子

发布包解压后，在根目录下有一个bin目录，提交任务相关的脚本都放在该目录下。例如运行简单的逻辑回归的例子：

```./angel-example com.tencent.angel.example.ml.LogisticRegLocalExample```

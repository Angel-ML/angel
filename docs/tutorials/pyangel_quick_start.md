# PyAngel快速入门


## 编写和编译

1. PyAngel的编写，推荐使用Atom或者pyCharm，高手请自备Vim或者Emacs
2. **编译代码**：请参考[Angel编译指南](../deploy/source_compile.md)，注意Python版本需要是3.6


## 运行任务

PyAngel有2种提交任务的模式

- **交互式**

	* **local模式**

		```bash
		bin/pyangel local
		```
	* **Yarn模式**

		```bash
		bin/pyangel
		```

- **脚本式**

	- **local模式**

		```bash
			bin/angel-local-submit --angel.pyangel.pyfile ${ANGEL_HOME}/python/examples/gbdt_local_example/py
		```

	- **Yarn模式**
	
		```bash
			bin/angel-submit --angel.pyangel.pyfile ${ANGEL_HOME}/python/examples/gbdt_example.py
		```



可以看出，PyAngel也支持2种运行模式：local & Yarn，Yarn模式依赖Hadoop，需要在机器上将Hadoop安装好，并且保证`HADOOP_HOME`设置正确，具体可以参考[Yarn运行模式]()

* **提交的样例命令**

```bash
	bin/angel-local-submit    --angel.pyangel.pyfile ${ANGEL_HOME}/python/examples/gbdt_local_example.py \
        			     	  --angel.train.data.path "file:///${ANGEL_HOME}/data/exampledata/GBDTLocalExampleData/agaricus.txt.train" \
        			     	  --angel.log.path "file:///${ANGEL_HOME}/data/log" \
        			     	  --angel.save.model.path "file:///${ANGEL_HOME}/data/output" \
```

## Example Code

* **PyAngel版本的GBDT**


	```Python

	from pyangel.ml.gbdt.runner import GBDTRunner

    # Trainning data input path
    input_path = "file:///${YOUR_ANGEL_HOME}/data/exampledata/GBDTLocalExampleData/agaricus.txt.train"
	
	# Algo param
	feature_num = 127
	feature_nzz = 25
	tree_num = 2
	tree_depth = 2
	split_num = 10
	sample_ratio = 1.0

	# Data format
	data_fmt = "libsvm"

	# Learning rate
	learn_rate = 0.01

    # Set GBDT trainning data path
    conf[AngelConf.ANGEL_TRAIN_DATA_PATH] = input_path
	# Set GBDT algorithm parameters
	conf[MLConf.ML_FEATURE_NUM] = str(feature_num)
	conf[MLConf.ML_FEATURE_NNZ] = str(feature_nzz)
	conf[MLConf.ML_GBDT_TREE_NUM] = str(tree_num)
	conf[MLConf.ML_GBDT_TREE_DEPTH] = str(tree_depth)
	conf[MLConf.ML_GBDT_SPLIT_NUM] = str(split_num)
	conf[MLConf.ML_GBDT_SAMPLE_RATIO] = str(sample_ratio)
	conf[MLConf.ML_LEARN_RATE] = str(learn_rate)

	runner = GBDTRunner()
	runner.train(conf)
	```

* [Demo实例](../../examples/src/main/python/gbdt_example.py)
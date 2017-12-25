# PyAngel快速入门

## 环境

* Linux任意发行版本,CentOS，Ubuntu等均可
* Angel >= 1.3
* Python >= 2.7 / 3.6（PyAngel支持2和3，但是建议3）

## 编写和编译

1. **编写**: 推荐使用Atom或者pyCharm，高手请自备Vim或者Emacs
2. **编译**：参考[Angel编译指南](../deploy/source_compile.md)，注意Python的版本


## 提交任务

PyAngel支持**交互式**和**脚本式**两种提交任务的模式，而每种提交任务的模式，都支持2种运行模式：**local & Yarn**。Yarn模式依赖Hadoop，需要在提交机器上将Hadoop安装好，并且保证`HADOOP_HOME`设置正确，具体可以参考[Yarn运行模式](../deploy/run_on_yarn.md)

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



### **样例命令**

* **Local模式提交**

  ```bash
  bin/angel-local-submit \
    --angel.pyangel.pyfile ${ANGEL_HOME}/python/examples/gbdt_local_example.py \
			  --angel.train.data.path "file:///${ANGEL_HOME}/data/exampledata/GBDTLocalExampleData/agaricus.txt.train" \
			  --angel.log.path "file:///${ANGEL_HOME}/data/log" \
			  --angel.save.model.path "file:///${ANGEL_HOME}/data/output"
	```

### Example Code

* **PyAngel版本的GBDT**
    
可以通过运行`bin/pyangel local`命令启动PyAngel本地交互式命令行，然后在命令行中输入下面的代码，运行GBDTRunner，注意：需要将input_path中的`${YOUR_ANGERL_HOME}`修改为你自己的angel绝对安装路径    

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

    # Set GBDT training data path
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

* [完整代码](../../angel-ps/examples/src/main/python/gbdt_example.py)

### 新版本

支持自定义Model，Task等操作，以及和Spark配合的相关功能正在开发中，如有疑问以及需求，欢迎提Issue和PR，或者联系Angel8号

# PyAngel Quick Start


## Environment

* Any Linux distribution, CentOS and Ubuntu, etc.
* Angel >= 1.3
* Python >= 2.7 / 3.6 (PyAngel supports Python 2 and 3, but Python3 is recommended)


## Programming and Compiling

1. **Programming**: Atom or pyCharm are recommended IDEs; otherwise, Vim or Emacs are fine
2. **Compiling**: refer to [Angel Compilation Guide](../deploy/source_compile_en.md); note the Python version


## Submitting Jobs

PyAngel supports **interactive** and **script** modes for submitting jobs. Each mode can run on **local** or **Yarn**. Running on Yarn requires Hadoop deployed to the submitting machine and `HADOOP_HOME` properly set. Refer to [Running Angel on Yarn](../deploy/run_on_yarn_en.md) for details. 

- **Interactive mode**

	* **local**

		```bash
		bin/pyangel local
		```
	* **Yarn**

		```bash
		bin/pyangel
		```

- **Script mode**

  - **local**

    ```bash
	bin/angel-local-submit --angel.pyangel.pyfile ${ANGEL_HOME}/python/examples/gbdt_local_example/py
	```

  - **Yarn**
	
	```bash
	bin/angel-submit --angel.pyangel.pyfile ${ANGEL_HOME}/python/examples/gbdt_example.py
	```



### **Sample Command**

* **Local**

  ```bash
  bin/angel-local-submit \
    --angel.pyangel.pyfile ${ANGEL_HOME}/python/examples/gbdt_local_example.py \
			  --angel.train.data.path "file:///${ANGEL_HOME}/data/exampledata/GBDTLocalExampleData/agaricus.txt.train" \
			  --angel.log.path "file:///${ANGEL_HOME}/data/log" \
			  --angel.save.model.path "file:///${ANGEL_HOME}/data/output"
	```

### Code Example

* **PyAngel version of GBDT**
    
Run `bin/pyangel local` to start PyAngel local interactive command-line, then input the sample code below to run GBDTRunner. Note: `${YOUR_ANGERL_HOME}` in input_path needs to be filled with the absolute path to where Angel is installed on your machine.

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
   Also, you can use a dict to set all the params Angel need:
```python
cate_feat = "0:2,1:2,2:2,3:2,4:2,5:2,6:2,7:2,8:2,9:2,10:2,11:2,12:2,13:2,14:2,15:2,16:2,17:2,18:2,19:2,20:2," \
    "21:2,22:2,23:2,24:2,25:2,26:2,27:2,28:2,29:2,30:2,31:2,32:2,33:2,34:2,35:2,36:2,37:2,38:2,39:2,40:2," \
    "41:2,42:2,43:2,44:2,45:2,46:2,47:2,48:2,49:2,50:2,51:2,52:2,53:2,54:2,55:2,56:2,57:2,58:2,59:2,60:2," \
    "61:2,62:2,63:2,64:2,65:2,66:2,67:2,68:2,69:2,70:2,71:2,72:2,73:2,74:2,75:2,76:2,77:2,78:2,79:2,80:2," \
    "81:2,82:2,83:2,84:2,85:2,86:2,87:2,88:2,89:2,90:2,91:2,92:2,93:2,94:2,95:2,96:2,97:2,98:2,99:2,100:2," \
    "101:2,102:2,103:2,104:2,105:2,106:2,107:2,108:2,109:2,110:2,111:2,112:2,113:2,114:2,115:2,116:2,117:2," \
    "118:2,119:2,120:2,121:2,122:2,123:2,124:2,125:2,126:2"
params = {
    AngelConf.ANGEL_DEPLOY_MODE: 'LOCAL',
    'mapred.mapper.new-api': True,
    AngelConf.ANGEL_INPUTFORMAT_CLASS: 'org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat',
    AngelConf.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST: True,
    AngelConf.ANGEL_WORKERGROUP_NUMBER: 1,
    AngelConf.ANGEL_WORKER_TASK_NUMBER: 1,
    AngelConf.ANGEL_PS_NUMBER: 1,
    MLConf.ML_DATA_FORMAT: 'libsvm',
    MLConf.ML_FEATURE_NUM: 127,
    MLConf.ML_FEATURE_NNZ: 25,
    MLConf.ML_GBDT_TREE_NUM: 2,
    MLConf.ML_GBDT_TREE_DEPTH: 2,
    MLConf.ML_GBDT_SPLIT_NUM: 10,
    MLConf.ML_GBDT_SAMPLE_RATIO: 1.0,
    MLConf.ML_LEARN_RATE: 0.01,
    MLConf.ML_GBDT_CATE_FEAT: cate_feat
}
self.conf.update(params)

runner = GBDTRunner()
runner.train(conf)
```   

* [Complete Code](../../angel-ps/examples/src/main/python/gbdt_example.py)

### New Version

Supports customized model/task operations. We are developing new functionalities that incorporates Spark. We welcome issue/PR for any questions or requests.

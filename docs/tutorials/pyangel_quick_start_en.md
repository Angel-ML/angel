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

* [Complete Code](../../angel-ps/examples/src/main/python/gbdt_example.py)

### New Version

Supports customized model/task operations. We are developing new functionalities that incorporates Spark. We welcome issue/PR for any questions or requests.

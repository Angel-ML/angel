# LDA（Latent Dirichlet Allocation）

---

> LDA is a widely-used topic-modeling technique, a Bayesian generative model for discovering hidden topical patterns that helps in dimension reduction and text analysis.

## 1. Introduction

### Overview

A text corpus ``$ C $`` contains a set of documents `` $ \{D_1, \cdots, D_{M}\} $``, and each document ``$ D_i $`` contains a set of words, ``$ D_i = (t_1, t_2, \cdots, t_{N_i}) $``. A word is a basic unit of a vocabulary denoted by ``$ V $``. The number of topics in LDA, ``$ K $``,  needs to be specified. In LDA, each document is modeled as a random mixture over ``$ K $`` latent topics, ``$ \theta_d $``, whereas each topic is modeled as a ``$ V $`` dimensional distribution over words, ``$ \phi_k $``.

LDA models the generative process for each document in the corpus. It draws a ``$ K $`` dimensional topic distribution, ``$ \theta_d $``, from a Dirichlet distribution, ``$ Dir(\alpha) $``, where ``$ \alpha $`` is the parameter vector of the Dirichlet (hyperparameter of the LDA). To generate each word ``$ t_{dn} $`` in document ``$ d $``, LDA first draws the topic of the word, ``$ z_{dn} $``, from a multinomial distribution ``$ Mult(\theta_d) $``, and then draws the word ``$ w_{dn} \in V $`` from a multinomial distribution ``$ Mult(\phi_{z_{dn}}) $``.

### Gibbs Sampling
A common inference technique for LDA is Gibbs Sampling, which is a MCMC method for sampling from the posterior distribution of ``$ z_{dn} $`` and infer the distribution over topics and the distribution over words for each document. Some commonly used Gibbs Sampling variants include the Collapsed Gibbs Sampling(CGS), SparseLDA, 
AliasLDA, F+LDA, LightLDA and WarpLDA, to name a few, and our experiment results suggest F+LDA as most suitable for training LDA on Angel. 

### Collapsed Gibbs Sampling (CGS)
We use ``$ Z=\{z_d\}_{d=1}^D $`` to represent the set of topics for all words, ``$ \Phi = [\phi_1 \cdots \phi_{V}] $`` to represent the ``$ V \times K $`` topic-word matrix, and ``$ \Theta = [\theta_1 \cdots \theta_D] $`` to represent the matrix whose columns are the topic distributions for all documents, then, training LDA requires inferring the posterior of the latent variable ``$ (\Theta, \Phi, Z) $``, given the observed variable ``$ Z $`` and the hyperparameters. Using conjugate prior, CGS gives a closed-form expression for the posterior of ``$ Z $``, resulting in simple iterations for sampling ``$ z_{dn} $`` following the conditional probability below:

```math
p(z_{dn} = k| t_{dn} = w, Z_{\neg dn}, C_{\neg dn}) \propto \\
			\frac{C_{wk}^{\neg dn} + \beta}{C_{k}^{\neg dn} \\
			+ V\beta}~(C_{dk}^{\neg dn}  + \alpha)
```

### F+LDA
F+LDA factorizes the probability into two parts, ``$ C_{dk} \frac{C_{wk} + \beta}{C_k + V\beta} $`` and ``$ \alpha \frac{C_{wk} + \beta}{C_k + V\beta} $``. Because ``$ C_d $`` is sparse, sampling will be only done for its non-zero elements; for the rest, F+LDA uses the F+ tree for searching, thus reducing the complexity to O(logK). Overall, F+LDA's complexity is ``$ O(K_d) $``, where ``$ K_d $`` is the number of non-zero elements in the document-topic matrix.

## 2. Distributed Implementation on Angel

The overall framework for training LDA on Angel is shown in the figure below. There are two comparatively large matrices in LDA, ``$ C_w $`` and ``$ C_d $``, and we slice C_d to different workers, and C_w to different servers. In each iteration, workers pull C_w from the servers for drawing topics, and send the updates on C_w back to the servers. 

![Architecture for LDA on Angel](../img/lda_ps.png)

## 3. Execution & Performance

### Input Format

* Each line is a document, and each document consists of a set of word ids; word ids are separated by `,`. 

```math
        wid_0 wid_1 ... wid_n
```

### Parameters

* Data Parameters
  * angel.train.data.path: input path
  * angel.save.model.path: save path for trained model 
* Algorithm Parameters
  * ml.epoch.num: number of iterations
  * ml.lda.word.num：number of words
  * ml.lda.topic.num：number of topics
  * ml.worker.thread.num：number of threads within each worker
  * ml.lda.alpha: alpha
  * ml.lda.beta: beta

### Submit Command

```shell
$ANGEL_HOME/bin/angel-submit \
     -Dangel.app.submit.class=com.tencent.angel.ml.lda.LDARunner \
     -Dangel.predict.data.path=$input\
     -Dmapreduce.input.fileinputformat.input.dir.recursive=true \
     -Dangel.load.model.path=$output \
     -Dangel.log.path=$log \
     -Dangel.predict.out.path=$predict \
     -Dsave.doc.topic=true\
     -Dsave.word.topic=true\
     -Dsave.doc.topic.distribution=true\
     -Dsave.topic.word.distribution=true\
     -Dml.lda.topic.num=1000 \
     -Dml.lda.word.num=141044 \
     -Dangel.worker.thread.num=8\
     -Dangel.worker.memory.mb=4000 \
     -Dangel.ps.memory.mb=4000 \
     -Dml.epoch.num=100 \
     -Dangel.worker.task.number=1 \
     -Dangel.ps.number=20 \
     -Dangel.workergroup.number=20 \
     -Dangel.am.log.level=INFO \
     -Daction.type=predict \
     -Dangel.worker.log.level=INFO \
     -Dangel.ps.log.level=INFO \
     -Dangel.task.data.storage.level=memory \
     -Dangel.worker.priority=5 \
     -Dqueue=$queue\
     -Dangel.output.path.deleteonexist=true \
     -Dangel.psagent.cache.sync.timeinterval.ms=100 \
     -Dangel.job.name=lda
``` 


### Performance
 
* **Data**
	 * PubMED 

* **Resource**
	* worker: 20
	* ps: 20

* **Angel vs Spark**: Training time with 100 iterations
	* Angel：15min
	* Spark：>300min


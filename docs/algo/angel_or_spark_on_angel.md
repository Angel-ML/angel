# Angel or Spark On Angel？
---
Angel拥有不同的运行模式：**ANGEL_PS_WORKER**和**ANGEL_PS**。

在**ANGEL_PS_WORKER**模式下，Angel可以独立完成模型的训练和预测等计算任务；在**ANGEL_PS**模式下，Angel启动PS服务，为其他的计算平台提供参数的存储和交换服务。目前基于**ANGEL_PS**运行模式，Angel开源社区打造了Spark On Angel计算平台。Spark On Angel比较好的解决了Spark ML的单点瓶颈，能够支持很大规模的模型训练。

由于历史版本的原因，Angel算法库和Spark On Angel算法库目前存在较大的重叠，但并非完全一致，那么在实际使用过程中，优先选择Angel还是Spark On Angel呢？下面从平台、算法和未来的趋势来谈一下选择的原则。

# 平台对比

Angel的优势是部署相对容易，它只需要部署计算平台，而Spark On Angel需要同时部署和使用两种计算平台。不过由于二者均为客户端部署模型，并不需要将相关的包安装到每一个计算节点，因此两者部署起来都不复杂。在计算性能方面，两者在大部分情况下性能相当，少数场景Angel略好。

不过在生态方面，Spark On Angel则是远好于Angel了。Spark On Angel拥有和Spark非常相似的编程接口，对于熟悉Spark的开发者而言门槛非常低；再者，Spark拥有非常强的数据分析能力，在数据预处理中广泛的使用，而Spark On Angel可以很方便的利用Spark在数据分析方面的优势，在一个平台中完成数据预处理和模型训练两阶段的计算任务。

因此，在算法库满足需求的前提下，建议使用Spark On Angel。

# 算法对比

# 未来发展趋势
未来Angel和Spark On Angel将有统一的算法库。Angel的Worker和Spark的Executor将只是运行算法库的容器。不过一个机器学习任务包含数据预处理，模型训练和模型服务等流程，最终模型效果不仅仅取决于模型训练，还取决于数据预处理，因此从机器学习全流程的角度，Spark On Angel无疑具有更大的优势，因此未来Angel开源社区也会在Spark On Angel的发展上投入更多的精力。



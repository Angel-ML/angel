**当前进展**
- 基于scala语言实现了论文前两部分：1.结构相似度计算的所有模块（包括DTW算法模块，Floyd算法模块以及相应的测试模块算法）2.多层网络构建的部分模块
- 待完成的为论文后续基于随机游走的采样的模块以及完成所有基于Spark编程并将功能模块嵌入
- 当前所完成的模块的入参（输入图）默认为邻接矩阵，由原始数据到此数据结构的代码仍有待完善
-
**Structure Similarity算法实现基本思路**:
- 输入图表示形式采用 adjacency matrix (|V| x | V|)，其中直接相邻顶点间边的权设为1，不直接相邻设为 INF (Integer.MAX_VALUE)
- 方便处理，假设输入图为单个连通分量，且不同顶点间无multiple edges
- 使用Floyd算法获得以上输入图的hopCountResult矩阵，显然在此矩阵中，最大值(单个连通分量，max hop count ！= INF)为此图直径，第i行记录了顶点i到到其他所有顶点的 hop counts
- 一次遍历hopCountResult矩阵，获取图直径以及各顶点度数
- 基于层次计算的思想，自底向上计算得到图结构相似度，最后输出结果为三维矩阵 Integer[k][n][n] structSimi，显然structSimi[k][i][j] = fk(Nodei,Nodej)

**算法需要进行的优化**:
- Item1：无向图的adjacency matrix为对称矩阵，可以进行相应的矩阵压缩
- Item2：DTW算法可根据论文https://go.exlibris.link/35X6ykDp 优化为快速DTW计算
- Item3：其他代码细节的优化 .etc
- 

**测试**
- 测试使用的图为 Barbell-Graph (1,1), 其对应邻接矩阵为：[[0,1,INF],[1,0,1],[INF,1,0]]
- 期望输出的相似度层次网络的矩阵（经手算验证）应为：[[[0,1,0],[1,0,1],[0,1,0]],[[0,3,0],[3,0,3],[0,3,0]],[[0,NaN,0],[NaN,NaN,NaN],[0,NaN,NaN]]]
- 相关具体输出结果见issue[1229]

------------更新于2022/9/11------------

**当前进展**
- 在第5、6周基本跑通local任务的基础上结合论文进行了进一步验证
- 最新的可视化结果表明算法实现基本正确
- 目前算法分布式版本任务除“Struc2VecPartition”和“Struc2VecPSModel”的实现外已基本完成
- 在Zachary’s Karate Network（第5&6周的测试输入图）的基础上构建Mirrored Zachary’s Karate Network(基于论文)
- 其中两个Zachary’s Karate Network由0号顶点与34号（0+33）顶点间的边所连结，即所有属于[0,33]号的顶点分别对应其顶点号+34的顶点所对应（例如：0 对应 34； 14 对应 48 ...）
- 输入Mirrored Zachary’s Karate network得到各个顶点的embedding，基于PCA得到压缩后的各顶点的二维向量，并使用Origin2021对得到的二维向量进行可视化

**struc2vec算法[local]实现基本思路**:
- 获得rawEdges的RDD后，使用collect()函数，建立图的完整邻接矩阵（local版本不进行切分）
- 基于类StructureSimilarity计算得到structure similarity distance network
- 建立multilayer weighted graph RDD
- 创建cross layer weight(level up weights)
- 创建the alias table
- sample node context,并注意如下实现细节: 
- scala中Double.NaN不等于任何数（包括其自身），判断时应该使用 “.isNaN()”
- 构建层间跳跃时，忽视某顶点的高一层对应顶点可能其邻点集合为空，在实现时判断逻辑优化为如下：
     -  在构建多层网络时在RDD中直接过滤掉邻点集为空的元素
     -  在进行跨层判断时，先判断该顶点是否存在上层对应点，若不存在，则跨层只向下


     
**测试**

1. 输入：
     - Mirrored Zachary’s Karate network（68 nodes 157 undirected edges）
     - walk length: 15
     - epochNum(训练轮数): 5
     - vector size (word2vec) : 10
     - window size (word2vec) : 3
     - stay(停留在当前层的概率): 0.5

2. 期望输出：
     - 在可视化散点图中所有属于[0,33]号的顶点分别与其对应其顶点号+34的顶点应该尽可能靠近（结构相似度最高）
     - 相关具体输出结果见issue[1242]

    
**未来的工作**:
- Item1：继续进行算法分布式版本实现的工作，特别是实现“Struc2VecPartition”和“Struc2VecPSModel”
- Item2：调整算法的超参数，优化得到的结果
- Item3：在算法整体实现完成后的基础上根据论文完善对实现代码细节的优化
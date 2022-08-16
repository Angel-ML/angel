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
- 基于层次计算的思想，自底向上计算得到图结构相似度，最后输入结果为三维矩阵 Integer[k][n][n] structSimi，显然structSimi[k][i][j] = fk(Nodei,Nodej)

**算法需要进行的优化**:
- Item1：无向图的adjacency matrix为对称矩阵，可以进行相应的矩阵压缩
- Item2：DTW算法可根据论文https://go.exlibris.link/35X6ykDp 优化为快速DTW计算
- Item3：其他代码细节的优化 .etc
- 

**测试**
- 测试使用的图为 Barbell-Graph (1,1), 其对应邻接矩阵为：[[0,1,INF],[1,0,1],[INF,1,0]]
- 期望输出的相似度层次网络的矩阵（经手算验证）应为：[[[0,1,0],[1,0,1],[0,1,0]],[[0,3,0],[3,0,3],[0,3,0]],[[0,NaN,0],[NaN,NaN,NaN],[0,NaN,NaN]]]
- 相关具体输出结果见issue


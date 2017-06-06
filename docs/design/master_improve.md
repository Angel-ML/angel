## 2. Master改进

* **资源申请策略**
	* 由于PS Cache需要与GPU卡运行在同一台物理机器上，所以在为PS Cache申请资源是必须将计算资源分配在GPU卡所在机器上

*  **管理策略**
	* PS Cache心跳维护，状态维护和down 机恢复等
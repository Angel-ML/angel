/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package com.tencent.angel.graph.client.initneighbor;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.graph.data.Node;
import com.tencent.angel.graph.data.NodeEdgesPair;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam;
import com.tencent.angel.psagent.PSAgentContext;

import java.util.*;

public class InitNeighborParam extends UpdateParam {
	private NodeEdgesPair[] nodeEdgesPairs;

	public InitNeighborParam(int matrixId, NodeEdgesPair[] nodeEdgesPairs) {
		super(matrixId);
		this.nodeEdgesPairs = nodeEdgesPairs;
	}

	@Override
	public List<PartitionUpdateParam> split() {
		Arrays.sort(nodeEdgesPairs);

		List<PartitionUpdateParam> partParams = new ArrayList<>();
		List<PartitionKey> partitions =
						PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);

		int nodeIndex = 0;
		int partIndex = 0;
		while (nodeIndex < nodeEdgesPairs.length || partIndex < partitions.size()) {
			int length = 0;
			long endOffset = partitions.get(partIndex).getEndCol();
			while (nodeIndex < nodeEdgesPairs.length && nodeEdgesPairs[nodeIndex].getNode().getId() < endOffset) {
				nodeIndex++;
				length++;
			}

			if (length > 0) {
				partParams.add(new PartInitNeighborParam(matrixId,
								partitions.get(partIndex), nodeEdgesPairs, nodeIndex - length, nodeIndex));
			}
			partIndex++;
		}

		return partParams;
	}
}

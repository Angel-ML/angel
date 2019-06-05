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

package com.tencent.angel.graph.ps.storage.vector;

import com.tencent.angel.graph.data.Edge;
import com.tencent.angel.graph.data.EdgeId;
import com.tencent.angel.graph.data.Node;
import com.tencent.angel.ps.storage.vector.ServerComplexTypeRow;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class GraphServerRow extends ServerComplexTypeRow {
	private final Map<Long, Node> id2Nodes;
	private final Map<EdgeId, Edge> id2Edges;

	public GraphServerRow(long startCol, long endCol) {
		super(null, 0, null, startCol, endCol, 0, null);
		id2Nodes = new TreeMap<>();
		id2Edges = new HashMap<>();
	}

	@Override
	public void init() {
	}

	public void addNode(Node node) {
		id2Nodes.put(node.getId(), node);
	}

	public void addEdge(Edge edge) {
		id2Edges.put(edge.getId(), edge);
	}

	public Node getNode(long id) {
		return id2Nodes.get(id);
	}

	public Edge getEdge(EdgeId id) {
		return id2Edges.get(id);
	}

	@Override
	public Object deepClone() {
		GraphServerRow clone = new GraphServerRow(startCol, endCol);
		for (Map.Entry<Long, Node> entry : id2Nodes.entrySet()) {
			clone.addNode((Node) entry.getValue().deepClone());
		}

		for (Map.Entry<EdgeId, Edge> entry : id2Edges.entrySet()) {
			clone.addEdge((Edge) entry.getValue().deepClone());
		}

		return clone;
	}
}

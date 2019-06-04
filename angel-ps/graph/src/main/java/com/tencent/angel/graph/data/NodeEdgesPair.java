package com.tencent.angel.graph.data;

public class NodeEdgesPair implements Comparable<NodeEdgesPair> {
    private final Node node;
    private final Edge[] edges;

    public NodeEdgesPair(Node node, Edge[] edges) {
        this.node = node;
        this.edges = edges;
    }

    public Node getNode() {
        return node;
    }

    public Edge[] getEdges() {
        return edges;
    }

    @Override
    public int compareTo(NodeEdgesPair o) {
        return Long.compare(node.id, o.node.id);
    }
}

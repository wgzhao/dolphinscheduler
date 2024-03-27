/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.dolphinscheduler.workflow.engine.dag;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * The IWorkflowDAG represent the DAG of a workflow.
 */
public class WorkflowDAG implements DAG {

    private final Map<NodeIdentify, Node> dagNodeMap;

    private final Map<NodeIdentify, List<Node>> outdegreeMap;

    private final Map<NodeIdentify, List<Node>> inDegredMap;

    public WorkflowDAG(List<Node> nodes, List<Edge> edges) {
        this.dagNodeMap = nodes.stream().collect(Collectors.toMap(Node::getNodeIdentify, Function.identity()));
        this.outdegreeMap = new HashMap<>();
        this.inDegredMap = new HashMap<>();
        // todo:
    }

    @Override
    public List<Node> getDirectPostNodes(Node dagNode) {
        NodeIdentify nodeIdentify = dagNode.getNodeIdentify();
        if (!dagNodeMap.containsKey(nodeIdentify)) {
            return Collections.emptyList();
        }
        Node node = dagNodeMap.get(nodeIdentify);
        List<Node> nodes = new ArrayList<>();
        for (DAGEdge edge : node.getOutDegrees()) {
            if (dagNodeMap.containsKey(edge.getToNodeName())) {
                nodes.add(dagNodeMap.get(edge.getToNodeName()));
            }
        }
        return nodes;
    }

    @Override
    public List<Node> getDirectPostNodes(String dagNodeName) {
        Node node = getDAGNode(dagNodeName);
        if (dagNodeName != null && node == null) {
            throw new IllegalArgumentException("Cannot find the Node: " + dagNodeName + " in DAG");
        }
        return getDirectPostNodes(node);
    }

    @Override
    public List<String> getDirectPostNodeNames(String dagNodeName) {
        Node node = getDAGNode(dagNodeName);
        if (dagNodeName != null && node == null) {
            throw new IllegalArgumentException("Cannot find the Node: " + dagNodeName + " in DAG");
        }
        return getDirectPostNodes(node).stream()
                .map(Node::getNodeName)
                .collect(Collectors.toList());
    }

    @Override
    public List<Node> getDirectPreNodes(Node dagNode) {
        final String nodeName = dagNode.getNodeName();
        if (!dagNodeMap.containsKey(nodeName)) {
            return Collections.emptyList();
        }
        Node node = dagNodeMap.get(nodeName);
        List<Node> nodes = new ArrayList<>();
        for (DAGEdge edge : node.getInDegrees()) {
            if (dagNodeMap.containsKey(edge.getFromNodeName())) {
                nodes.add(dagNodeMap.get(edge.getFromNodeName()));
            }
        }
        return nodes;
    }

    @Override
    public List<Node> getDirectPreNodes(String dagNodeName) {
        Node node = getDAGNode(dagNodeName);
        if (dagNodeName != null && node == null) {
            throw new IllegalArgumentException("Cannot find the Node: " + dagNodeName + " in DAG");
        }
        return getDirectPreNodes(node);
    }

    @Override
    public List<String> getDirectPreNodeNames(String dagNodeName) {
        Node node = getDAGNode(dagNodeName);
        if (dagNodeName != null && node == null) {
            throw new IllegalArgumentException("Cannot find the Node: " + dagNodeName + " in DAG");
        }
        return getDirectPreNodes(node).stream().map(Node::getNodeName).collect(Collectors.toList());
    }

    @Override
    public Node getDAGNode(String nodeName) {
        return dagNodeMap.get(nodeName);
    }

}

package org.apache.dolphinscheduler.workflow.engine.dag;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;

import org.junit.jupiter.api.Test;

import com.google.common.collect.Lists;

class NodeTest {

    @Test
    void buildDAGNode_EmptyNodeName() {
        IllegalArgumentException illegalArgumentException = assertThrows(IllegalArgumentException.class,
                () -> Node.builder()
                        .inDegrees(new ArrayList<>())
                        .outDegrees(new ArrayList<>())
                        .build());
        assertEquals("nodeName cannot be empty", illegalArgumentException.getMessage());
    }

    @Test
    void buildDAGNode_BadInDegree() {
        IllegalArgumentException illegalArgumentException = assertThrows(IllegalArgumentException.class,
                () -> Node.builder()
                        .nodeName("A")
                        .inDegrees(Lists.newArrayList(DAGEdge.builder()
                                .fromNodeName(null)
                                .toNodeName("B")
                                .build()))
                        .outDegrees(new ArrayList<>())
                        .build());
        assertEquals(
                "The toNodeName of inDegree should be the nodeName of the node: A, inDegree: DAGEdge(fromNodeName=null, toNodeName=B)",
                illegalArgumentException.getMessage());
    }

    @Test
    void buildDAGNode_NiceInDegree() {
        assertDoesNotThrow(() -> Node.builder()
                .nodeName("A")
                .inDegrees(Lists.newArrayList(DAGEdge.builder()
                        .fromNodeName(null)
                        .toNodeName("A")
                        .build()))
                .outDegrees(new ArrayList<>())
                .build());
    }

    @Test
    void buildDAGNode_BadOutDegree() {
        IllegalArgumentException illegalArgumentException = assertThrows(IllegalArgumentException.class,
                () -> Node.builder()
                        .nodeName("A")
                        .inDegrees(new ArrayList<>())
                        .outDegrees(Lists.newArrayList(DAGEdge.builder()
                                .fromNodeName("B")
                                .toNodeName(null)
                                .build()))
                        .build());
        assertEquals(
                "The fromNodeName of outDegree should be the nodeName of the node: A, outDegree: DAGEdge(fromNodeName=B, toNodeName=null)",
                illegalArgumentException.getMessage());
    }

    @Test
    void buildDAGNode_NiceOutDegree() {
        assertDoesNotThrow(() -> Node.builder()
                .nodeName("A")
                .inDegrees(new ArrayList<>())
                .outDegrees(Lists.newArrayList(DAGEdge.builder()
                        .fromNodeName("A")
                        .toNodeName(null)
                        .build()))
                .build());
    }

    @Test
    void buildDAGNode_NotSkip() {
        Node node = Node.builder()
                .nodeName("A")
                .inDegrees(new ArrayList<>())
                .outDegrees(new ArrayList<>())
                .build();
        assertFalse(node.isSkip());
    }

    @Test
    void buildDAGNode_Skip() {
        Node node = Node.builder()
                .nodeName("A")
                .skip(true)
                .inDegrees(new ArrayList<>())
                .outDegrees(new ArrayList<>())
                .build();
        assertTrue(node.isSkip());
    }

}

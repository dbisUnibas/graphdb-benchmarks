package eu.socialsensor.insert;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.*;

public class TinkerPopSingleInsertionTinkerGraphTest {
    Graph graph;

    @Before
    public void setUp() throws Exception {
        graph = TinkerGraph.open();
    }

    @Test
    public void getOrCreate() {
        assertEquals(0, (long) graph.traversal().V().count().next());
        TinkerPopSingleInsertionTinkerGraph ins =
                new TinkerPopSingleInsertionTinkerGraph(
                        graph, new File("."));
        TinkerPopInsertionTestCases.getOrCreate(graph, ins);
    }

    @Test
    public void relateNodes() {
        assertEquals(0, (long) graph.traversal().E().count().next());
        TinkerPopSingleInsertionTinkerGraph ins =
                new TinkerPopSingleInsertionTinkerGraph(
                        graph, new File("."));
        TinkerPopInsertionTestCases.relateNodes(graph, ins);
    }
}
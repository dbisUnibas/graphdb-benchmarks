package eu.socialsensor.insert;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.*;

public class TinkerPopMassiveInsertionTinkerGraphTest {
    Graph graph;

    @After
    public void tearDown() throws Exception {
        graph.close();
    }

    @Before
    public void setUp() throws Exception {
        graph = TinkerGraph.open();

    }

    @Test
    public void getOrCreate() {
        assertEquals(0, (long) graph.traversal().V().count().next());
        InsertionBase<Vertex> ins =
                new TinkerPopMassiveInsertionTinkerGraph(
                        graph, new File("."));
        TinkerPopInsertionTestCases.getOrCreate(graph, ins);
    }

    @Test
    public void relateNodes() {
        assertEquals(0, (long) graph.traversal().V().count().next());
        InsertionBase<Vertex> ins =
                new TinkerPopMassiveInsertionTinkerGraph(
                        graph, new File("."));
        TinkerPopInsertionTestCases.relateNodes(graph, ins);
    }
}
package eu.socialsensor.insert;

import eu.socialsensor.graphdatabases.GraphDatabaseBase;
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
        assertEquals(0, (long) graph.traversal().V().count().next());
        Vertex v;
        v = ins.getOrCreate("test");
        assertEquals(1, (long) graph.traversal().V().count().next());
        assertEquals("test", v.property(GraphDatabaseBase.NODE_ID).value());
        v = ins.getOrCreate("test");
        assertEquals(1, (long) graph.traversal().V().count().next());
        assertEquals("test", v.property(GraphDatabaseBase.NODE_ID).value());
        v = ins.getOrCreate("test2");
        assertEquals(2, (long) graph.traversal().V().count().next());
        assertEquals("test2", v.property(GraphDatabaseBase.NODE_ID).value());
        v = ins.getOrCreate("test2");
        assertEquals(2, (long) graph.traversal().V().count().next());
        assertEquals("test2", v.property(GraphDatabaseBase.NODE_ID).value());
    }

    @Test
    public void relateNodes() {
        assertEquals(0, (long) graph.traversal().E().count().next());
        TinkerPopSingleInsertionTinkerGraph ins =
                new TinkerPopSingleInsertionTinkerGraph(
                        graph, new File("."));
        assertEquals(0, (long) graph.traversal().E().count().next());
        Vertex v1 = ins.getOrCreate("test");
        Vertex v2 = ins.getOrCreate("test2");
        ins.relateNodes(v1, v2);
        assertEquals(1, (long) graph.traversal().E().count().next());
        ins.relateNodes(v1, v2);
        assertEquals(2, (long) graph.traversal().E().count().next()); // we assume that multiple relationships are ok
    }

    @Test
    public void createGraph() {
    }
}
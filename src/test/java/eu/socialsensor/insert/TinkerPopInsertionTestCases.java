package eu.socialsensor.insert;

import eu.socialsensor.graphdatabases.GraphDatabaseBase;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.io.File;

import static org.junit.Assert.assertEquals;

public class TinkerPopInsertionTestCases {
    static void getOrCreate(Graph graph, InsertionBase<Vertex> ins) {

        assertEquals(0, (long) graph.traversal().V().count().next());
        Vertex v;
        v = ins.getOrCreate("0");
        assertEquals(1, (long) graph.traversal().V().count().next());
        assertEquals(0, v.property(GraphDatabaseBase.NODE_ID).value());
        v = ins.getOrCreate("0");
        assertEquals(1, (long) graph.traversal().V().count().next());
        assertEquals(0, v.property(GraphDatabaseBase.NODE_ID).value());
        v = ins.getOrCreate("1");
        assertEquals(2, (long) graph.traversal().V().count().next());
        assertEquals(1, v.property(GraphDatabaseBase.NODE_ID).value());
        v = ins.getOrCreate("1");
        assertEquals(2, (long) graph.traversal().V().count().next());
        assertEquals(1, v.property(GraphDatabaseBase.NODE_ID).value());
    }

    static void relateNodes(Graph graph, InsertionBase<Vertex> ins) {
        assertEquals(0, (long) graph.traversal().E().count().next());

        assertEquals(0, (long) graph.traversal().E().count().next());
        Vertex v1 = ins.getOrCreate("0");
        Vertex v2 = ins.getOrCreate("1");
        ins.relateNodes(v1, v2);
        assertEquals(1, (long) graph.traversal().E().count().next());
        ins.relateNodes(v1, v2);
        assertEquals(2, (long) graph.traversal().E().count().next()); // we assume that multiple relationships are ok
    }

}

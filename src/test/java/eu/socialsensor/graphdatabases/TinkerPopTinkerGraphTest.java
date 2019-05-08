package eu.socialsensor.graphdatabases;

import eu.socialsensor.insert.TinkerPopSingleInsertionBase;
import eu.socialsensor.insert.TinkerPopSingleInsertionTinkerGraph;
import eu.socialsensor.main.BenchmarkConfiguration;
import eu.socialsensor.main.BenchmarkingException;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;

import static org.junit.Assert.*;

public class TinkerPopTinkerGraphTest {
    TinkerPopTinkerGraph db;
    TinkerPopSingleInsertionTinkerGraph ins;

    @Before
    public void setUp() throws Exception {
        Configuration conf = new BaseConfiguration();
        conf.addProperty("eu.socialsensor.database-storage-directory", ".");
        conf.addProperty("eu.socialsensor.dataset", "Email-Enron.txt");
        conf.addProperty("eu.socialsensor.permute-benchmarks", false);
        conf.addProperty("eu.socialsensor.results-path", "testresults");
        BenchmarkConfiguration config = new BenchmarkConfiguration(conf);
        db = new TinkerPopTinkerGraph(config, config.getDbStorageDirectory());
    }
    @Test
    public void getNodeCount() {
        db.open();
        assertEquals(0, db.getNodeCount());
        db.graph.addVertex("test");
        assertEquals(1, db.getNodeCount());

    }

    @Test
    public void getVertexIterator() {
        db.open();
        assertEquals(0, db.getNodeCount());
        db.graph.addVertex("test1");
        db.graph.addVertex("test2");
        db.graph.addVertex("test3");
        Iterator<Vertex> it = db.getVertexIterator();

    }

    @Test
    public void open() {
        db.open();
    }

    @Test
    public void shutdown() {
        db.open();
        db.shutdown();
    }

    @Test
    public void delete() {
        db.open();
        db.shutdown();
    }

    @Test
    public void getOtherVertexFromEdge() {
        db.open();
        Vertex v1 = db.graph.addVertex("test1");
        Vertex v2 = db.graph.addVertex("test2");
        Vertex v3 = db.graph.addVertex("test3");

        Edge e1 = v1.addEdge("label", v2);
        Edge e2 = v1.addEdge("label", v3);

        assertEquals(v1, db.getOtherVertexFromEdge(e1, v2));
        assertEquals(v2, db.getOtherVertexFromEdge(e1, v1));
        assertEquals(v3, db.getOtherVertexFromEdge(e2, v1));
        assertEquals(v1, db.getOtherVertexFromEdge(e2, v3));
        // now the nonsense input should throw or return nothing
        try {
            assertNull(db.getOtherVertexFromEdge(e2, v2));
            fail();
        } catch (BenchmarkingException e) {
        }
    }

    @Test
    public void getSrcVertexFromEdge() {
        db.open();
        Vertex v1 = db.graph.addVertex("test1");
        Vertex v2 = db.graph.addVertex("test2");
        Vertex v3 = db.graph.addVertex("test3");

        Edge e1 = v1.addEdge("label", v2);
        Edge e2 = v1.addEdge("label", v3);
        Edge e3 = v3.addEdge("label", v1);

        assertEquals(v1, db.getSrcVertexFromEdge(e1));
        assertEquals(v1, db.getSrcVertexFromEdge(e2));
        assertEquals(v3, db.getSrcVertexFromEdge(e3));

    }

    @Test
    public void getDestVertexFromEdge() {
        db.open();
        Vertex v1 = db.graph.addVertex("test1");
        Vertex v2 = db.graph.addVertex("test2");
        Vertex v3 = db.graph.addVertex("test3");

        Edge e1 = v1.addEdge("label", v2);
        Edge e2 = v1.addEdge("label", v3);
        Edge e3 = v3.addEdge("label", v1);

        assertEquals(v2, db.getDestVertexFromEdge(e1));
        assertEquals(v3, db.getDestVertexFromEdge(e2));
        assertEquals(v1, db.getDestVertexFromEdge(e3));
    }

    @Test
    public void getVertex() {
        db.open();
        Vertex v1 = db.graph.addVertex(TinkerPopSingleInsertionBase.NODE_LABEL);
        v1.property(GraphDatabaseBase.NODE_ID, 1);
        Vertex v2 = db.graph.addVertex(TinkerPopSingleInsertionBase.NODE_LABEL);
        v2.property(GraphDatabaseBase.NODE_ID, "2");

        assertEquals(v1, db.getVertex(1));
        try {
            assertEquals(v2, db.getVertex(2));
            fail();
        } catch (BenchmarkingException e) {
        }
    }

    @Test
    public void shortestPath() {
        //todo
        fail();
    }

    @Test
    public void getNeighborsIds() {
        db.open();
        Vertex v1 = db.graph.addVertex(TinkerPopSingleInsertionBase.NODE_LABEL);
        v1.property(GraphDatabaseBase.NODE_ID, 1);
        Vertex v2 = db.graph.addVertex(TinkerPopSingleInsertionBase.NODE_LABEL);
        v2.property(GraphDatabaseBase.NODE_ID, 2);
        Vertex v3 = db.graph.addVertex(TinkerPopSingleInsertionBase.NODE_LABEL);
        v3.property(GraphDatabaseBase.NODE_ID, 3);
        Vertex v4 = db.graph.addVertex(TinkerPopSingleInsertionBase.NODE_LABEL);
        v4.property(GraphDatabaseBase.NODE_ID, 4);

        Edge e1 = v1.addEdge("label", v2);
        Edge e2 = v1.addEdge("label", v3);
        Edge e3 = v3.addEdge("label", v1);
        Edge e4 = v3.addEdge("label", v1);
        // self is not neighbor
        assertFalse(db.getNeighborsIds(1).contains(1));

        assertEquals(2, db.getNeighborsIds(1).size());

        assertFalse(db.getNeighborsIds(1).contains(4));
        assertTrue(db.getNeighborsIds(1).contains(2));
        assertTrue(db.getNeighborsIds(1).contains(3));

        // 2 only incoming edges
        assertTrue(db.getNeighborsIds(2).isEmpty());

        // 4 is not connected
        assertTrue(db.getNeighborsIds(4).isEmpty());
    }

    @Test
    public void getNodeWeight() {
        //todo
        fail();
    }

    @Test
    public void nodeExists() {
        //todo
        fail();
    }
}
package eu.socialsensor.graphdatabases;

import eu.socialsensor.insert.TinkerPopSingleInsertionBase;
import eu.socialsensor.insert.TinkerPopSingleInsertionTinkerGraph;
import eu.socialsensor.main.BenchmarkConfiguration;
import eu.socialsensor.main.BenchmarkingException;
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.function.Function;

import static eu.socialsensor.graphdatabases.GraphDatabaseBase.*;
import static eu.socialsensor.insert.TinkerPopSingleInsertionBase.NODE_LABEL;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.has;
import static org.junit.Assert.*;
import static org.junit.Assert.assertTrue;

public class TinkerPopTinkerGraphTest {
    TinkerPopBase db;
    void createCommunityGraph() {
        List<Vertex> vertices = new ArrayList<>();
        for (int  i = 0; i < 10; i++) {
            vertices.add(db.graph.addVertex(T.label, NODE_LABEL, NODE_ID, i));
        }

        vertices.get(0).addEdge(SIMILAR, vertices.get(1));
        vertices.get(0).addEdge(SIMILAR, vertices.get(2));
        vertices.get(0).addEdge(SIMILAR, vertices.get(3));
        vertices.get(2).addEdge(SIMILAR, vertices.get(6));
        vertices.get(3).addEdge(SIMILAR, vertices.get(1));
        vertices.get(4).addEdge(SIMILAR, vertices.get(5));
        vertices.get(7).addEdge(SIMILAR, vertices.get(0));
        vertices.get(7).addEdge(SIMILAR, vertices.get(1));
        vertices.get(7).addEdge(SIMILAR, vertices.get(8));
        vertices.get(8).addEdge(SIMILAR, vertices.get(9));
        vertices.get(9).addEdge(SIMILAR, vertices.get(8));

        vertices.get(0).property(COMMUNITY, 0);
        vertices.get(0).property(NODE_COMMUNITY, 0);
        vertices.get(1).property(COMMUNITY, 0);
        vertices.get(1).property(NODE_COMMUNITY, 0);
        vertices.get(2).property(COMMUNITY, 2);
        vertices.get(2).property(NODE_COMMUNITY, 2);
        vertices.get(3).property(COMMUNITY, 0);
        vertices.get(3).property(NODE_COMMUNITY, 0);
        vertices.get(4).property(COMMUNITY, 4);
        vertices.get(4).property(NODE_COMMUNITY, 4);
        vertices.get(5).property(COMMUNITY, 4);
        vertices.get(5).property(NODE_COMMUNITY, 4);
        vertices.get(6).property(COMMUNITY, 2);
        vertices.get(6).property(NODE_COMMUNITY, 2);
        vertices.get(7).property(COMMUNITY, 0);
        vertices.get(7).property(NODE_COMMUNITY, 0);
        vertices.get(8).property(COMMUNITY, 8);
        vertices.get(8).property(NODE_COMMUNITY, 8);
        vertices.get(9).property(COMMUNITY, 8);
        vertices.get(9).property(NODE_COMMUNITY, 8);

//        db.graph.traversal().V().properties().sideEffect(System.out::println).iterate();
//        db.graph.traversal().V().bothE().path().sideEffect(System.out::println).iterate();
    }

    @Before
    public void setUp() throws Exception {
        Configuration conf = new BaseConfiguration();
        conf.addProperty("eu.socialsensor.database-storage-directory", "databasedata");
        conf.addProperty("eu.socialsensor.dataset", "data/Email-Enron.txt");
        conf.addProperty("eu.socialsensor.permute-benchmarks", false);
        conf.addProperty("eu.socialsensor.results-path", "testresults");
        BenchmarkConfiguration config = new BenchmarkConfiguration(conf);
        db = new TinkerPopTinkerGraph(config, config.getDbStorageDirectory());
//        db = new TinkerPopNeo4j(config, config.getDbStorageDirectory());
        db.open();
    }

    @After
    public void tearDown() throws Exception {
        db.shutdown();
        db.delete();
    }

    @Test
    public void getNodeCount() {
        assertEquals(0, db.getNodeCount());
        db.graph.addVertex("test");
        assertEquals(1, db.getNodeCount());

    }

    @Test
    public void getVertexIterator() {
        assertEquals(0, db.getNodeCount());
        db.graph.addVertex("test1");
        db.graph.addVertex("test2");
        db.graph.addVertex("test3");
        Iterator<Vertex> it = db.getVertexIterator();

    }


    @Test
    public void getOtherVertexFromEdge() {
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
        Vertex v1 = db.graph.addVertex(NODE_LABEL);
        v1.property(NODE_ID, 1);
        Vertex v2 = db.graph.addVertex(NODE_LABEL);
        v2.property(NODE_ID, "2");

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
        createCommunityGraph();
        Vertex v7 = db.graph.traversal().V().has(NODE_ID, 7).next();
        db.shortestPath(v7, 1);
    }

    @Test
    public void getNeighborsIds() {
        Vertex v1 = db.graph.addVertex(NODE_LABEL);
        v1.property(NODE_ID, 1);
        Vertex v2 = db.graph.addVertex(NODE_LABEL);
        v2.property(NODE_ID, 2);
        Vertex v3 = db.graph.addVertex(NODE_LABEL);
        v3.property(NODE_ID, 3);
        Vertex v4 = db.graph.addVertex(NODE_LABEL);
        v4.property(NODE_ID, 4);

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
        Vertex v1 = db.graph.addVertex(NODE_LABEL);
        v1.property(NODE_ID, 1);
        Vertex v2 = db.graph.addVertex(NODE_LABEL);
        v2.property(NODE_ID, 2);
        Vertex v3 = db.graph.addVertex(NODE_LABEL);
        v3.property(NODE_ID, 3);
        Vertex v4 = db.graph.addVertex(NODE_LABEL);
        v4.property(NODE_ID, 4);

        Edge e1 = v1.addEdge("label", v2);
        Edge e2 = v1.addEdge("label", v3);
        Edge e3 = v3.addEdge("label", v1);
        Edge e4 = v3.addEdge("label", v1);
        assertEquals(2, (int) db.getNodeWeight(1));
        assertEquals(0, (int) db.getNodeWeight(2));
        assertEquals(2, (int) db.getNodeWeight(3));
        assertEquals(0, (int) db.getNodeWeight(4));

    }

    @Test
    public void nodeExists() {
        Vertex v1 = db.graph.addVertex(NODE_LABEL);
        v1.property(NODE_ID, 1);
        Vertex v2 = db.graph.addVertex(NODE_LABEL);
        v2.property(NODE_ID, 2);
        Vertex v3 = db.graph.addVertex(NODE_LABEL);
        v3.property(NODE_ID, 3);
        Vertex v4 = db.graph.addVertex(NODE_LABEL);
        assertFalse(db.nodeExists(4));
        v4.property(NODE_ID, 4);
        assertTrue(db.nodeExists(1));
        assertTrue(db.nodeExists(2));
        assertTrue(db.nodeExists(3));
        assertTrue(db.nodeExists(4));
        assertFalse(db.nodeExists(5));
        assertFalse(db.nodeExists(0));
        assertFalse(db.nodeExists(-1));

    }

    @Test
    public void getGraphWeightSum() {
        Vertex v1 = db.graph.addVertex(NODE_LABEL);
        v1.property(NODE_ID, 1);
        Vertex v2 = db.graph.addVertex(NODE_LABEL);
        v2.property(NODE_ID, 2);
        Vertex v3 = db.graph.addVertex(NODE_LABEL);
        v3.property(NODE_ID, 3);
        Vertex v4 = db.graph.addVertex(NODE_LABEL);
        v4.property(NODE_ID, 4);

        Edge e1 = v1.addEdge("label", v2);
        Edge e2 = v1.addEdge("label", v3);
        Edge e3 = v3.addEdge("label", v1);
        Edge e4 = v3.addEdge("label", v1);
        assertEquals(4, (int) db.getGraphWeightSum());
    }

    @Test
    public void initCommunityProperty() {
        for (int  i = 0; i < 10; i++) {
            db.graph.addVertex(T.label, NODE_LABEL, NODE_ID, i);
        }
        db.initCommunityProperty();
        assertEquals(10, db.graph.traversal().V().values(COMMUNITY).toSet().size());
        assertEquals(10, db.graph.traversal().V().values(NODE_COMMUNITY).toSet().size());
    }

    @Test
    public void getCommunitiesConnectedToNodeCommunities() {
        createCommunityGraph();
        Set<Integer> intSet0 = db.getCommunitiesConnectedToNodeCommunities(0);
        assertTrue(intSet0.contains(2));
        assertTrue(intSet0.contains(0));
        assertTrue(intSet0.contains(8));

        Set<Integer> intSet1 = db.getCommunitiesConnectedToNodeCommunities(1);
        assertTrue(intSet1.isEmpty());

        Set<Integer> intSet4 = db.getCommunitiesConnectedToNodeCommunities(4);
        assertTrue(intSet4.contains(4));
        assertEquals(1, intSet4.size());
    }

    @Test
    public void getNodesFromCommunity() {
        createCommunityGraph();
        Set<Integer> intSet0 = db.getNodesFromCommunity(0);
        int[] elems = {0, 1, 3, 7};
        for (int i: elems) {
            assertTrue(intSet0.contains(i));
        }

        Set<Integer> intSet2 = db.getNodesFromCommunity(2);
        int[] elems2 = {2, 6};
        for (int i: elems2) {
            assertTrue(intSet2.contains(i));
        }

        Set<Integer> intSet8 = db.getNodesFromCommunity(8);
        int[] elems8 = {8, 9};
        for (int i: elems8) {
            assertTrue(intSet8.contains(i));
        }
        Set<Integer> intSet4 = db.getNodesFromCommunity(4);
        int[] elems4 = {4, 5};
        for (int i: elems4) {
            assertTrue(intSet4.contains(i));
        }
    }

    @Test
    public void getNodesFromNodeCommunity() {
        createCommunityGraph();
        Set<Integer> intSet0 = db.getNodesFromNodeCommunity(0);
        int[] elems = {0, 1, 3, 7};
        for (int i: elems) {
            assertTrue(intSet0.contains(i));
        }

        Set<Integer> intSet2 = db.getNodesFromNodeCommunity(2);
        int[] elems2 = {2, 6};
        for (int i: elems2) {
            assertTrue(intSet2.contains(i));
        }

        Set<Integer> intSet8 = db.getNodesFromNodeCommunity(8);
        int[] elems8 = {8, 9};
        for (int i: elems8) {
            assertTrue(intSet8.contains(i));
        }
        Set<Integer> intSet4 = db.getNodesFromNodeCommunity(4);
        int[] elems4 = {4, 5};
        for (int i: elems4) {
            assertTrue(intSet4.contains(i));
        }
    }

    @Test
    public void getEdgesInsideCommunity() {
        createCommunityGraph();
        assertEquals(5, (int) db.getEdgesInsideCommunity(0, 0));
        assertEquals(0, (int) db.getEdgesInsideCommunity(1, 1));
        assertEquals(1, (int) db.getEdgesInsideCommunity(2, 2));
        assertEquals(1, (int) db.getEdgesInsideCommunity(0, 8));
        assertEquals(0, (int) db.getEdgesInsideCommunity(0, 5));
        assertEquals(0, (int) db.getEdgesInsideCommunity(32, 8));
    }

    @Test
    public void getCommunityWeight() {
        createCommunityGraph();
        assertEquals(7, (int) db.getCommunityWeight(0));
        assertEquals(0, (int) db.getCommunityWeight(1));
        assertEquals(1, (int) db.getCommunityWeight(2));
        assertEquals(2, (int) db.getCommunityWeight(8));
        assertEquals(1, (int) db.getCommunityWeight(4));
    }

    @Test
    public void getNodeCommunityWeight() {
        createCommunityGraph();
        assertEquals(7, (int) db.getNodeCommunityWeight(0));
        assertEquals(0, (int) db.getNodeCommunityWeight(1));
        assertEquals(1, (int) db.getNodeCommunityWeight(2));
        assertEquals(2, (int) db.getNodeCommunityWeight(8));
        assertEquals(1, (int) db.getNodeCommunityWeight(4));
    }

    @Test
    public void moveNode() {
        createCommunityGraph();
        db.moveNode(0, 1);
        List<Vertex> movedNodes = db.graph.traversal().V().has(NODE_COMMUNITY, 0).toList();
        for (Vertex v: movedNodes) {
            assertEquals(1, (int) v.value(COMMUNITY));
            assertEquals(0, (int) v.value(NODE_COMMUNITY));
        }
        List<Vertex> untouched = db.graph.traversal().V().not(has(NODE_COMMUNITY, 0)).toList();
        for (Vertex v: untouched) {
            assertEquals((int) v.value(NODE_COMMUNITY), (int) v.value(COMMUNITY));
        }
    }

    @Test
    public void reInitializeCommunities() {
        createCommunityGraph();
        db.reInitializeCommunities();

    }

    @Test
    public void getCommunity() {
    }

    @Test
    public void getCommunityFromNode() {
    }

    @Test
    public void getCommunitySize() {
    }

    @Test
    public void mapCommunities() {
    }

    @Test
    public void getNeighborsOfVertex() {
        Function<Vertex, List<Edge>> neighborIt2List = (v) -> {
            Iterator<Edge> it = db.getNeighborsOfVertex(v);
            List<Edge> ev = IteratorUtils.toList(it);
            db.cleanupEdgeIterator(it);
            return ev;
        };
        Vertex v1 = db.graph.addVertex(NODE_LABEL);
        v1.property(NODE_ID, 1);
        Vertex v2 = db.graph.addVertex(NODE_LABEL);
        v2.property(NODE_ID, 2);
        Vertex v3 = db.graph.addVertex(NODE_LABEL);
        v3.property(NODE_ID, 3);
        Vertex v4 = db.graph.addVertex(NODE_LABEL);
        v4.property(NODE_ID, 4);

        Edge e1 = v1.addEdge("label", v2);
        Edge e2 = v1.addEdge("label", v3);
        Edge e3 = v3.addEdge("label", v1);
        Edge e4 = v3.addEdge("label", v1);

        List<Edge> ev1 = neighborIt2List.apply(v1);
        assertTrue(ev1.contains(e1));
        assertTrue(ev1.contains(e2));
        assertTrue(ev1.contains(e3));
        assertTrue(ev1.contains(e4));


        assertEquals(4, ev1.size());

        // 2 only edge to 1
        List<Edge> ev2 = neighborIt2List.apply(v2);
        assertTrue(ev2.contains(e1));
        assertEquals(1, ev2.size());

        // 4 is not connected
        List<Edge> ev4 = neighborIt2List.apply(v4);
        assertTrue(ev4.isEmpty());
    }
}
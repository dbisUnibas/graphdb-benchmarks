package eu.socialsensor.graphdatabases;

import eu.socialsensor.insert.TinkerPopSingleInsertionBase;
import eu.socialsensor.main.BenchmarkingException;
import eu.socialsensor.main.GraphDatabaseType;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.io.File;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;

public abstract class TinkerPopBase extends GraphDatabaseBase<Iterator<Vertex>, Iterator<Edge>, Vertex, Edge> {
    Graph graph;
    private int numEdgeIterators = 0; //to track wheter it is safe to commit
    private int numVertexIterators = 0;

    public TinkerPopBase(GraphDatabaseType type, File dbStorageDirectory) {
        super(type, dbStorageDirectory);
    }

    private void commitIfSupported() {
        if (graph.features().graph().supportsTransactions() && numEdgeIterators == 0 && numVertexIterators == 0) {
            graph.tx().commit();
        }
    }

    private void rollbackIfSupported() {
        if (graph.features().graph().supportsTransactions()) {
            graph.tx().rollback();
        }
    }
    @Override
    public int getNodeCount() {
        long nodeCount = graph.traversal().V().count().next();
        if (nodeCount > Integer.MAX_VALUE) {
            throw new BenchmarkingException("Node count too high for int representation");
        }
        return (int) nodeCount;
    }

    @Override
    public Iterator<Vertex> getVertexIterator() {
        Iterator<Vertex> it = graph.vertices();
        numVertexIterators++;
        return it;
    }

    @Override
    public Iterator<Edge> getAllEdges() {
        Iterator<Edge> it = graph.edges();
        numEdgeIterators++;
        return it;
    }

    @Override
    public Iterator<Edge> getNeighborsOfVertex(Vertex v) {
//        graph.vertices(v).next().edges(Direction.BOTH);
        // todo: what's the difference of accessing via traversal() or something like vertices(v)? above vs. below
        // I'm worried about side-effects on the latter and maybe poor performance on the former but i don't have
        // a clue, really... I think we need iterate() to actually do the traversal... True, but iterate()
        // does not return anything... as Traverals implement the iterator interface, we can just return that...
        Iterator<Edge> it = graph.traversal().V(v).bothE();
        numEdgeIterators++;
        return it;
    }

    @Override
    public Vertex getOtherVertexFromEdge(Edge r, Vertex oneVertex) {
        try {
            Vertex v = graph.traversal().V(oneVertex).bothE().filter(has(T.id, r.id())).otherV().next();
            commitIfSupported();
            return v;
        } catch (NoSuchElementException e) {
            rollbackIfSupported();
            throw new BenchmarkingException(
                    String.format("oneVertex '%s' is not connected to Edge '%s'!", oneVertex, r));
        }
    }

    @Override
    public Vertex getSrcVertexFromEdge(Edge edge) {
        Vertex v = graph.traversal().E(edge).outV().next();
        commitIfSupported();
        return v;
    }

    @Override
    public Vertex getDestVertexFromEdge(Edge edge) {
        Vertex v = graph.traversal().E(edge).inV().next();
        commitIfSupported();
        return v;
    }

    @Override
    public Vertex getVertex(Integer i) {
        try {
            Vertex v = graph.traversal().V().hasLabel(TinkerPopSingleInsertionBase.NODE_LABEL).has(NODE_ID, i).next();
            commitIfSupported();
            return v;
        } catch (NoSuchElementException e) {
            rollbackIfSupported();
            throw new BenchmarkingException(String.format("No Vertex with id '%d' in graph", i));
        }
    }

    @Override
    public boolean edgeIteratorHasNext(Iterator<Edge> it) {
        return it.hasNext();
    }

    @Override
    public Edge nextEdge(Iterator<Edge> it) {
        return it.next();
    }

    @Override
    public boolean vertexIteratorHasNext(Iterator<Vertex> it) {
        return it.hasNext();
    }

    @Override
    public Vertex nextVertex(Iterator<Vertex> it) {
        return it.next();
    }

    @Override
    public Set<Integer> getNeighborsIds(int nodeId) {
        // todo: I copied behavior from neo4j implementation (also for getNeighbors()). But getNeighborsOfV()
        // todo: considers both directions, whereas this method only considers outgoing. Is this intentional?
        Set<Integer> neighborIds = new HashSet<>();
        List <Vertex> neighborNodes = graph.traversal().V().hasLabel(TinkerPopSingleInsertionBase.NODE_LABEL)
                .has(NODE_ID, nodeId).out().toList();
        for(Vertex v: neighborNodes) {
            neighborIds.add((Integer) v.property(NODE_ID).value());
        }
        commitIfSupported();
        return neighborIds;
    }

    @Override
    public void shortestPath(Vertex fromNode, Integer node) {
        // the following assumes that the first path found is the shortest.
        graph.traversal().V(fromNode).repeat(out().simplePath()).until(
                hasLabel(TinkerPopSingleInsertionBase.NODE_LABEL).has(NODE_ID, node)).path().limit(1).iterate();
        commitIfSupported();
        // todo: test
    }

    @Override
    public double getNodeWeight(int nodeId) {
        double weight = graph.traversal().V().hasLabel(TinkerPopSingleInsertionBase.NODE_LABEL)
                        .has(NODE_ID, nodeId).outE().count().next(); //todo test
        commitIfSupported();
        return weight;
    }

    @Override
    public void cleanupEdgeIterator(Iterator<Edge> it) {
        numEdgeIterators--;
        commitIfSupported(); // we can't commit here already otherwise all iterators invalidated!
    }

    @Override
    public void cleanupVertexIterator(Iterator<Vertex> it) {
        numVertexIterators--;
        commitIfSupported();
    }

    @Override
    public boolean nodeExists(int nodeId) {
        boolean exists = graph.traversal().V().hasLabel(TinkerPopSingleInsertionBase.NODE_LABEL).has(NODE_ID, nodeId).hasNext();
        commitIfSupported();
        return exists;
    }

    @Override
    public double getGraphWeightSum() {
        double weight = graph.traversal().E().count().next();
        commitIfSupported();
        return weight;
    }

    @Override
    public void initCommunityProperty() {
        int community = 0;
        Iterator<Vertex> v = graph.vertices();
        while (v.hasNext()) {
            Vertex ve = v.next();
            ve.property(COMMUNITY, community);
            ve.property(NODE_COMMUNITY, community);
            community++;
        }
        commitIfSupported();
        //todo: test
    }

    @Override
    public Set<Integer> getCommunitiesConnectedToNodeCommunities(int nodeCommunities) {
        Set<Object> set = graph.traversal().V()
                .hasLabel(TinkerPopSingleInsertionBase.NODE_LABEL).has(NODE_COMMUNITY, nodeCommunities)
                .outE(SIMILAR).otherV().properties(NODE_COMMUNITY).value().toSet();
        commitIfSupported();
        return set.stream().mapToInt(i -> Integer.parseInt((String) i)).boxed().collect(Collectors.toSet());
    }
    //todo: test

    @Override
    public Set<Integer> getNodesFromCommunity(int community) {
        Set<Object> set = graph.traversal().V()
                .hasLabel(TinkerPopSingleInsertionBase.NODE_LABEL).has(COMMUNITY, community)
                .outE(SIMILAR).otherV().properties(COMMUNITY).value().toSet();
        commitIfSupported();
        return set.stream().mapToInt(i -> Integer.parseInt((String) i)).boxed().collect(Collectors.toSet());
    }

    @Override
    public Set<Integer> getNodesFromNodeCommunity(int nodeCommunity) {
        Set<Object> set = graph.traversal().V()
                .hasLabel(TinkerPopSingleInsertionBase.NODE_LABEL).has(NODE_COMMUNITY, nodeCommunity)
                .properties(NODE_ID).value().toSet();
        commitIfSupported();
        return set.stream().mapToInt(i -> Integer.parseInt((String) i)).boxed().collect(Collectors.toSet());
    }

    @Override
    public double getEdgesInsideCommunity(int nodeCommunity, int communityNodes) {
        double count = graph.traversal().V()
                .hasLabel(TinkerPopSingleInsertionBase.NODE_LABEL).has(NODE_COMMUNITY,nodeCommunity)
                .outE(SIMILAR).as("edge")
                .otherV().has(COMMUNITY, communityNodes).select("edge").count().next();
        commitIfSupported();
        return count; //todo: test that. Most likely wrong...
    }
}
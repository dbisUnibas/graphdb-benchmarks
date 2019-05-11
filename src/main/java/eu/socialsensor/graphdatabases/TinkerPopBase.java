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

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;

public abstract class TinkerPopBase extends GraphDatabaseBase<Iterator<Vertex>, Iterator<Edge>, Vertex, Edge> {
    Graph graph;

    public TinkerPopBase(GraphDatabaseType type, File dbStorageDirectory) {
        super(type, dbStorageDirectory);
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
        return graph.vertices();
    }

    @Override
    public Iterator<Edge> getAllEdges() {
        return graph.edges();
    }

    @Override
    public Iterator<Edge> getNeighborsOfVertex(Vertex v) {
//        graph.vertices(v).next().edges(Direction.BOTH);
        // todo: what's the difference of accessing via traversal() or something like vertices(v)? above vs. below
        // I'm worried about side-effects on the latter and maybe poor performance on the former but i don't have
        // a clue, really... I think we need iterate() to actually do the traversal... True, but iterate()
        // does not return anything... as Traverals implement the iterator interface, we can just return that...
        return graph.traversal().V(v).bothE();
    }

    @Override
    public Vertex getOtherVertexFromEdge(Edge r, Vertex oneVertex) {
        try {
            return graph.traversal().V(oneVertex).bothE().filter(has(T.id, r.id())).otherV().next();
        } catch (NoSuchElementException e) {
            throw new BenchmarkingException(
                    String.format("oneVertex '%s' is not connected to Edge '%s'!", oneVertex, r));
        }
    }

    @Override
    public Vertex getSrcVertexFromEdge(Edge edge) {
        return graph.traversal().E(edge).outV().next();
    }

    @Override
    public Vertex getDestVertexFromEdge(Edge edge) {
        return graph.traversal().E(edge).inV().next();
    }

    @Override
    public Vertex getVertex(Integer i) {
        try {
            return graph.traversal().V().hasLabel(TinkerPopSingleInsertionBase.NODE_LABEL).has(NODE_ID, i).next(); //todo test
        } catch (NoSuchElementException e) {
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
        return neighborIds;
    }

    @Override
    public void shortestPath(Vertex fromNode, Integer node) {
        // the following assumes that the first path found is the shortest.
        graph.traversal().V(fromNode).repeat(out().simplePath()).until(
                hasLabel(TinkerPopSingleInsertionBase.NODE_LABEL).has(NODE_ID, node)).path().limit(1).iterate();
        // todo: test
    }

    @Override
    public double getNodeWeight(int nodeId) {
        return graph.traversal().V().hasLabel(TinkerPopSingleInsertionBase.NODE_LABEL)
                .has(NODE_ID, nodeId).outE().count().next(); //todo test
    }

    @Override
    public void cleanupEdgeIterator(Iterator<Edge> it) {
        //NOOP
    }

    @Override
    public void cleanupVertexIterator(Iterator<Vertex> it) {
        //NOOP
    }

    @Override
    public boolean nodeExists(int nodeId) {
        return graph.traversal().V().hasLabel(TinkerPopSingleInsertionBase.NODE_LABEL).has(NODE_ID, nodeId).hasNext();
        //todo test

    }
}

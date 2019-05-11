package eu.socialsensor.graphdatabases;

import eu.socialsensor.insert.TinkerPopMassiveInsertionTinkerGraph;
import eu.socialsensor.insert.TinkerPopSingleInsertionBase;
import eu.socialsensor.insert.TinkerPopSingleInsertionTinkerGraph;
import eu.socialsensor.main.BenchmarkConfiguration;
import eu.socialsensor.main.BenchmarkingException;
import eu.socialsensor.main.GraphDatabaseType;
import eu.socialsensor.utils.Utils;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.hasLabel;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;

public class TinkerPopTinkerGraph extends TinkerPopBase {

    public TinkerPopTinkerGraph(BenchmarkConfiguration config, File dbStorageDirectory) {
        super(GraphDatabaseType.TINKERPOP_TINKERGRAPH, dbStorageDirectory);
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
    public void open() {
        Configuration conf = new BaseConfiguration();
        conf.addProperty(TinkerGraph.GREMLIN_TINKERGRAPH_GRAPH_LOCATION,
                new File(dbStorageDirectory, "tinkerGraph.gryo").toString());
        conf.addProperty(TinkerGraph.GREMLIN_TINKERGRAPH_GRAPH_FORMAT, "gryo"); // we need to persist to be able
        this.graph = TinkerGraph.open(conf); // to use data from insertion in later benchmarks
    }

    @Override
    public void createGraphForSingleLoad() {
        // delete before starting, otherwise this benchmark makes no sense, as data will already be loaded.
        // apparently framework doesn't do this? huh?
        delete();
        open();
        ((TinkerGraph) graph).createIndex(NODE_ID, Vertex.class); // index is essential for insert speed!
    }

    @Override
    public void massiveModeLoading(File dataPath) {
        TinkerPopMassiveInsertionTinkerGraph ins = new TinkerPopMassiveInsertionTinkerGraph(this.graph, null);
        ins.createGraph(dataPath, 0); //what's with these parameters?? NEO4j impl sets them 0/null...

    }

    @Override
    public void singleModeLoading(File dataPath, File resultsPath, int scenarioNumber) {
        TinkerPopSingleInsertionTinkerGraph ins = new TinkerPopSingleInsertionTinkerGraph(this.graph, resultsPath);
        ins.createGraph(dataPath, scenarioNumber);
    }

    @Override
    public void createGraphForMassiveLoad() {
        // delete before starting, otherwise this benchmark makes no sense, as data will already be loaded.
        // apparently framework doesn't do this? huh?
        delete();
        open();
        ((TinkerGraph) graph).createIndex(NODE_ID, Vertex.class); // index is essential for insert speed!
    }

    @Override
    public void shutdown() { // persist DB
        try {
            graph.close();
        } catch (Exception e) {
            throw new BenchmarkingException(String.format("Can't close TinkerGraph! %s", e.getMessage()));
        }
    }

    @Override
    public void delete() {
        System.out.println("Deleting DB");
        Utils.deleteRecursively(dbStorageDirectory);
    }

    @Override
    public void shutdownMassiveGraph() {
        shutdown();
    }

    @Override
    public void shortestPath(Vertex fromNode, Integer node) {
        // the following assumes that the first path found is the shortest.
        System.out.println(graph.traversal().V(fromNode).repeat(out().simplePath()).until(
                hasLabel(TinkerPopSingleInsertionBase.NODE_LABEL).has(NODE_ID, node)).path().limit(1).iterate());
        // todo: test
    }

    @Override
    public double getNodeWeight(int nodeId) {
        return graph.traversal().V().hasLabel(TinkerPopSingleInsertionBase.NODE_LABEL)
                .has(NODE_ID, nodeId).outE().count().next(); //todo test
    }

    @Override
    public void initCommunityProperty() {
        throw new RuntimeException("initCommunityProperty not implemented");
        //todo

    }

    @Override
    public Set<Integer> getCommunitiesConnectedToNodeCommunities(int nodeCommunities) {
        throw new RuntimeException("getCommunitiesConnectedToNodeCommunities not implemented");

//        return null; //todo
    }

    @Override
    public Set<Integer> getNodesFromCommunity(int community) {
        throw new RuntimeException("getNodesFromCommunity not implemented");
//        return null; //todo
    }

    @Override
    public Set<Integer> getNodesFromNodeCommunity(int nodeCommunity) {
        throw new RuntimeException("getNodesFromNodeCommunity not implemented");
//        return null; //todo
    }

    @Override
    public double getEdgesInsideCommunity(int nodeCommunity, int communityNodes) {
        throw new RuntimeException("getEdgesInsideCommunity not implemented");
//        return 0; //todo
    }

    @Override
    public double getCommunityWeight(int community) {
        throw new RuntimeException("getCommunityWeight not implemented");
//        return 0; //todo
    }

    @Override
    public double getNodeCommunityWeight(int nodeCommunity) {
        throw new RuntimeException("getNodeCommunityWeight not implemented");
//        return 0; //todo
    }

    @Override
    public void moveNode(int from, int to) {
        throw new RuntimeException("moveNode not implemented");
//todo

    }

    @Override
    public double getGraphWeightSum() {
        throw new RuntimeException("getGraphWeightSum not implemented");
//        return 0;        //todo

    }

    @Override
    public int reInitializeCommunities() {
        throw new RuntimeException("reInitializeCommunities not implemented");
//        return 0;        //todo

    }

    @Override
    public int getCommunityFromNode(int nodeId) {
        throw new RuntimeException("getCommunityFromNode not implemented");
//        return 0;        //todo

    }

    @Override
    public int getCommunity(int nodeCommunity) {
        throw new RuntimeException("getCommunity not implemented");
//        return 0;        //todo

    }

    @Override
    public int getCommunitySize(int community) {
        throw new RuntimeException("getCommunitySize not implemented");
//        return 0;        //todo

    }

    @Override
    public Map<Integer, List<Integer>> mapCommunities(int numberOfCommunities) {
        throw new RuntimeException("mapCommunities not implemented");
//        return null;        //todo

    }

    @Override
    public boolean nodeExists(int nodeId) {
        return graph.traversal().V().hasLabel(TinkerPopSingleInsertionBase.NODE_LABEL).has(NODE_ID, nodeId).hasNext();
        //todo test

    }
}
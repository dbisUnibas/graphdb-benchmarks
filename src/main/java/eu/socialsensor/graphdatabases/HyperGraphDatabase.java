package eu.socialsensor.graphdatabases;


import eu.socialsensor.main.BenchmarkConfiguration;
import eu.socialsensor.main.GraphDatabaseType;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Hyper Graph  database implementation
 *
 * @author Fabrizio Parrillo
 */
public class HyperGraphDatabase extends GraphDatabaseBase<Iterable<Object>, Iterable<Object>, Object, Object> {

    public HyperGraphDatabase(BenchmarkConfiguration config, File dbStorageDirectory) {
        super(GraphDatabaseType.HYPERGRAPH_DB, dbStorageDirectory);
    }

    @Override
    public Object getOtherVertexFromEdge(Object r, Object oneVertex) {
       // HGUtils.getImplementationOf(HGStoreImplementation.)

    return null;
    }

    @Override
    public Object getSrcVertexFromEdge(Object edge) {
        return null;
    }

    @Override
    public Object getDestVertexFromEdge(Object edge) {
        return null;
    }

    @Override
    public Object getVertex(Integer i) {
        return null;
    }

    @Override
    public Iterable<Object> getAllEdges() {
        return null;
    }

    @Override
    public Iterable<Object> getNeighborsOfVertex(Object v) {
        return null;
    }

    @Override
    public boolean edgeIteratorHasNext(Iterable<Object> it) {
        return false;
    }

    @Override
    public Object nextEdge(Iterable<Object> it) {
        return null;
    }

    @Override
    public void cleanupEdgeIterator(Iterable<Object> it) {

    }

    @Override
    public Iterable<Object> getVertexIterator() {
        return null;
    }

    @Override
    public boolean vertexIteratorHasNext(Iterable<Object> it) {
        return false;
    }

    @Override
    public Object nextVertex(Iterable<Object> it) {
        return null;
    }

    @Override
    public void cleanupVertexIterator(Iterable<Object> it) {

    }

    @Override
    public void open() {

    }

    @Override
    public void createGraphForSingleLoad() {

    }

    @Override
    public void massiveModeLoading(File dataPath) {

    }

    @Override
    public void singleModeLoading(File dataPath, File resultsPath, int scenarioNumber) {

    }

    @Override
    public void createGraphForMassiveLoad() {

    }

    @Override
    public void shutdown() {

    }

    @Override
    public void delete() {

    }

    @Override
    public void shutdownMassiveGraph() {

    }

    @Override
    public void shortestPath(Object fromNode, Integer node) {

    }

    @Override
    public int getNodeCount() {
        return 0;
    }

    @Override
    public Set<Integer> getNeighborsIds(int nodeId) {
        return null;
    }

    @Override
    public double getNodeWeight(int nodeId) {
        return 0;
    }

    @Override
    public void initCommunityProperty() {

    }

    @Override
    public Set<Integer> getCommunitiesConnectedToNodeCommunities(int nodeCommunities) {
        return null;
    }

    @Override
    public Set<Integer> getNodesFromCommunity(int community) {
        return null;
    }

    @Override
    public Set<Integer> getNodesFromNodeCommunity(int nodeCommunity) {
        return null;
    }

    @Override
    public double getEdgesInsideCommunity(int nodeCommunity, int communityNodes) {
        return 0;
    }

    @Override
    public double getCommunityWeight(int community) {
        return 0;
    }

    @Override
    public double getNodeCommunityWeight(int nodeCommunity) {
        return 0;
    }

    @Override
    public void moveNode(int from, int to) {

    }

    @Override
    public double getGraphWeightSum() {
        return 0;
    }

    @Override
    public int reInitializeCommunities() {
        return 0;
    }

    @Override
    public int getCommunityFromNode(int nodeId) {
        return 0;
    }

    @Override
    public int getCommunity(int nodeCommunity) {
        return 0;
    }

    @Override
    public int getCommunitySize(int community) {
        return 0;
    }

    @Override
    public Map<Integer, List<Integer>> mapCommunities(int numberOfCommunities) {
        return null;
    }

    @Override
    public boolean nodeExists(int nodeId) {
        return false;
    }
}

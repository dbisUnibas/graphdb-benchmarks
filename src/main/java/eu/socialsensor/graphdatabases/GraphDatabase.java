package eu.socialsensor.graphdatabases;


import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * Represents a graph database
 *
 * @author sotbeis, sotbeis@iti.gr
 * @author Alexander Patrikalakis
 */
public interface GraphDatabase<VertexIteratorType, EdgeIteratorType, VertexType, EdgeType> {

    //edge and vertex operations
    VertexType getOtherVertexFromEdge( EdgeType r, VertexType oneVertex );

    VertexType getSrcVertexFromEdge( EdgeType edge );

    VertexType getDestVertexFromEdge( EdgeType edge );

    VertexType getVertex( Integer i );

    //edge iterators
    EdgeIteratorType getAllEdges();

    EdgeIteratorType getNeighborsOfVertex( VertexType v );

    boolean edgeIteratorHasNext( EdgeIteratorType it );

    EdgeType nextEdge( EdgeIteratorType it );

    void cleanupEdgeIterator( EdgeIteratorType it );

    //vertex iterators
    VertexIteratorType getVertexIterator();

    boolean vertexIteratorHasNext( VertexIteratorType it );

    VertexType nextVertex( VertexIteratorType it );

    void cleanupVertexIterator( VertexIteratorType it );

    //benchmarks
    void findAllNodeNeighbours();

    void findNodesOfAllEdges();

    /**
     * Opens the graph database
     */
    void open();

    /**
     * Creates a graph database and configures for single data insertion
     */
    void createGraphForSingleLoad();

    /**
     * Inserts data in massive mode
     *
     * @param dataPath - dataset path
     */
    void massiveModeLoading( File dataPath );

    /**
     * Inserts data in single mode
     *
     * @param dataPath - dataset path
     */
    void singleModeLoading( File dataPath, File resultsPath, int scenarioNumber );

    /**
     * Creates a graph database and configures for bulk data insertion
     */
    void createGraphForMassiveLoad();

    /**
     * Shut down the graph database
     */
    void shutdown();

    /**
     * Delete the graph database
     */
    void delete();

    /**
     * Shutdown the graph database, which configuration is for massive insertion
     * of data
     */
    void shutdownMassiveGraph();

    /**
     * Find the shortest path between vertex 1 and each of the vertexes in the list
     *
     * @param nodes any number of random nodes
     */
    void shortestPaths( Set<Integer> nodes );

    /**
     * Execute findShortestPaths query from the Query interface
     */
    void shortestPath( final VertexType fromNode, Integer node );

    /**
     * @return the number of nodes
     */
    int getNodeCount();

    /**
     * @return the neighbours of a particular node
     */
    Set<Integer> getNeighborsIds( int nodeId );

    /**
     * @return the node degree
     */
    double getNodeWeight( int nodeId );

    /**
     * Initializes the community and nodeCommunity property in each database
     */
    void initCommunityProperty();

    /**
     * @return the communities (communityId) that are connected with a
     * particular nodeCommunity
     */
    Set<Integer> getCommunitiesConnectedToNodeCommunities( int nodeCommunities );

    /**
     * @return the nodes a particular community contains
     */
    Set<Integer> getNodesFromCommunity( int community );

    /**
     * @return the nodes a particular nodeCommunity contains
     */
    Set<Integer> getNodesFromNodeCommunity( int nodeCommunity );

    /**
     * @return the number of edges between a community and a nodeCommunity
     */
    double getEdgesInsideCommunity( int nodeCommunity, int communityNodes );

    /**
     * @return the sum of node degrees
     */
    double getCommunityWeight( int community );

    /**
     * @return the sum of node degrees
     */
    double getNodeCommunityWeight( int nodeCommunity );

    /**
     * Moves a node from a community to another
     */
    void moveNode( int from, int to );

    /**
     * @return the number of edges of the graph database
     */
    double getGraphWeightSum();

    /**
     * Reinitializes the community and nodeCommunity property
     *
     * @return the number of communities
     */
    int reInitializeCommunities();

    /**
     * @return in which community a particular node belongs
     */
    int getCommunityFromNode( int nodeId );

    /**
     * @return in which community a particular nodeCommunity belongs
     */
    int getCommunity( int nodeCommunity );

    /**
     * @return the number of nodeCommunities a particular community contains
     */
    int getCommunitySize( int community );

    /**
     * @return a map where the key is the community id and the value is the
     * nodes each community has.
     */
    Map<Integer, List<Integer>> mapCommunities( int numberOfCommunities );

    /**
     * @return return true if node exist, false if not
     */
    boolean nodeExists( int nodeId );
}

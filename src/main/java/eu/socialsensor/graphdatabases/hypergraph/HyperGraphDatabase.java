package eu.socialsensor.graphdatabases.hypergraph;


import eu.socialsensor.graphdatabases.GraphDatabaseBase;
import eu.socialsensor.graphdatabases.hypergraph.edge.RelTypeSimilar;
import eu.socialsensor.graphdatabases.hypergraph.vertex.Node;
import eu.socialsensor.graphdatabases.hypergraph.vertex.NodeQueries;
import eu.socialsensor.insert.HyperGraphdataSingleInsertion;
import eu.socialsensor.insert.Insertion;
import eu.socialsensor.main.BenchmarkConfiguration;
import eu.socialsensor.main.GraphDatabaseType;
import eu.socialsensor.utils.Utils;
import org.apache.commons.lang.NotImplementedException;
import org.hypergraphdb.HGConfiguration;
import org.hypergraphdb.HGEnvironment;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.algorithms.DefaultALGenerator;
import org.hypergraphdb.algorithms.GraphClassics;
import org.hypergraphdb.algorithms.HGALGenerator;
import org.hypergraphdb.atom.HGRel;

import java.io.File;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Hyper Graph  database implementation
 *
 * @author Fabrizio Parrillo
 */
public class HyperGraphDatabase
    extends
    GraphDatabaseBase
        <
        Iterable<Node>,
        Iterator<HGRel>,
        Node,
        HGRel
        >
{

  protected HyperGraph graph = null;

  public HyperGraphDatabase(
      BenchmarkConfiguration config,
      File dbStorageDirectory
  ) {
    super(
        GraphDatabaseType.HYPERGRAPH_DB,
        dbStorageDirectory
    );
  }

  private static List<HGHandle> extractShortestPath(
      Map<HGHandle, HGHandle> predecessorMap,
      HGHandle start,
      HGHandle goal
  ) {

    List<HGHandle> shortestPath = new LinkedList<HGHandle>();
    HGHandle currentPredecessor = goal;
    while (currentPredecessor != start) {
      currentPredecessor = predecessorMap.get(currentPredecessor);
      shortestPath.add(currentPredecessor);
    }
    return shortestPath;
  }

  /**
   * List<HGHandle> HGHandles = graph.getAll(similarNeighborOfNode);
   * <p>
   * Iterator<HGHandle> neighbors = HGHandles.stream() // The node might occur in multiple hyper
   * edges, // but we are only interested in the outgoing edges // with respect to the link
   * condition (Node we are looking at). .filter(link -> link.getTargetAt(0).equals(v))
   * .findFirst().get().iterator();
   **//*

    }*/
  @Override
  public Node getOtherVertexFromEdge(
      HGRel r,
      Node oneVertex
  ) {
    throw new NotImplementedException();
  }

  @Override
  public Node getSrcVertexFromEdge
      (
          HGRel edge
      ) {
    throw new NotImplementedException();
  }

  @Override
  public Node getDestVertexFromEdge
      (
          HGRel edge
      ) {
    throw new NotImplementedException();
  }

  @Override
  public Node getVertex
      (
          Integer nodeId
      ) {
    throw new NotImplementedException();
  }

  @Override
  public Iterator<HGRel> getAllEdges
      (
      ) {
    HGHandle areSimilar = RelTypeSimilar.getHGRelType(graph);
    return graph.<HGRel>getAll(hg.and(hg.type(areSimilar))).iterator();
  }

  @Override
  public Iterator<HGRel>
  getNeighborsOfVertex
      (
          Node v
      ) {
    throw new NotImplementedException();
  }

  @Override
  public boolean edgeIteratorHasNext
      (
          Iterator<HGRel> it
      ) {
    throw new NotImplementedException();
  }

  @Override
  public HGRel nextEdge
      (
          Iterator<HGRel> it
      ) {
    throw new NotImplementedException();
  }

  @Override
  public void cleanupEdgeIterator
      (
          Iterator<HGRel> it
      ) {
    throw new NotImplementedException();
  }

  @Override
  public Iterable<Node> getVertexIterator
      (
      ) {
    return  graph.getAll(NodeQueries.nodeType());
  }

/*    @Override
    public Iterator<HyperGraphDatabase.Node> getNeighborsOfVertex(Node v) {
        LinkCondition vertex = hg.link(hg.assertAtom(graph, v));
        AtomTypeCondition edge = hg.type(this.similarRelType);
        HGQueryCondition similarNeighborOfNode = hg.and(vertex, edge);

        HGHandle link = graph.getOne(similarNeighborOfNode);
        Iterator<HGHandle> neighbors = link.iterator();
        neighbors.next();
        return null;

        */

  @Override
  public boolean vertexIteratorHasNext
      (
          Iterable<Node> it
      ) {
    throw new NotImplementedException();
  }

  @Override
  public Node nextVertex(
      Iterable<Node> it
  ) {
    throw new NotImplementedException();
  }

  @Override
  public void cleanupVertexIterator
      (
          Iterable<Node> it
      ) {
    throw new NotImplementedException();
  }

  @Override
  public void open
      (
      ) {
    createHyperGraphDB();
  }

  @Override
  public void createGraphForSingleLoad() {
    createHyperGraphDB();
    createSchema();
  }

  @Override
  public void massiveModeLoading(File dataPath) {
    throw new NotImplementedException();
  }

  @Override
  public void singleModeLoading(File dataPath, File resultsPath, int scenarioNumber) {
    Insertion insertion = new HyperGraphdataSingleInsertion(graph, resultsPath);
    insertion.createGraph(dataPath, scenarioNumber);
  }

  @Override
  public void createGraphForMassiveLoad() {
    throw new NotImplementedException();
  }

  @Override
  public void shutdown() {
    if (graph == null) {
      return;
    }
    graph.close();
    graph = null;
  }

  @Override
  public void delete() {
    Utils.deleteRecursively(dbStorageDirectory);
  }

  @Override
  public void shutdownMassiveGraph() {
    shutdown();
  }

  @Override
  public void shortestPath(Node fromNode, Integer nodeId) {
    // adjacency list (AL) generator describes which links and atoms are visited for the search. In many cases you might want to define a custom one
    HGALGenerator adjGen = new DefaultALGenerator(graph);
    // Unless you only care about the length of the shortest path, you have to create a result map beforehand, so you can access the result later.
    Map<HGHandle, HGHandle> predecessorMap = new HashMap<>();

    HGHandle start = graph.getOne(NodeQueries.queryById(fromNode.getId()));
    HGHandle goal = graph.getOne(NodeQueries.queryById(nodeId));
    double paths = GraphClassics.dijkstra(
        start,        // start atom
        goal,        // goal atom
        adjGen,
        null,     // weights of links: null if all links count equal
        null,
        // distance Matrix: In most cases it's ok to put null here, since you don't need to know the distances between all atoms. The length of the shortest path will be returned by the method

        predecessorMap  // not null, unless only length of shortest path is required);
    );
    List<HGHandle> shortesPath = HyperGraphDatabase
        .extractShortestPath(predecessorMap, start, goal);
    shortesPath.size();

  }

  @Override
  public int getNodeCount() {
    return (int) graph.count(NodeQueries.nodeType());
  }

  @Override
  public Set<Integer> getNeighborsIds(int nodeId) {
    throw new NotImplementedException();
  }

  @Override
  public double getNodeWeight(int nodeId) {
    hg.getOne(graph, NodeQueries.queryById(nodeId));
    return 0;
  }

  @Override
  public void initCommunityProperty() {
    int communityCounter = 0;
    for (Object o : graph.getAll(NodeQueries.nodeType())) {
      Node n = (Node) o;
      n.setNodeCommunity(communityCounter);
      n.setCommunity(communityCounter);
      graph.update(n);
      communityCounter++;
    }
  }

  @Override
  public Set<Integer> getCommunitiesConnectedToNodeCommunities(int nodeCommunities) {
    throw new NotImplementedException();
  }

  @Override
  public Set<Integer> getNodesFromCommunity(int community) {
    return graph.getAll(NodeQueries.queryByCommunity(community)).stream()
        .map(o -> ((Node) o).getId())
        .collect(Collectors.toSet());
  }

  @Override
  public Set<Integer> getNodesFromNodeCommunity(int nodeCommunity) {
    return graph.getAll(NodeQueries.queryByNodeCommunity(nodeCommunity)).stream()
        .map(o -> ((Node) o).getId())
        .collect(Collectors.toSet());
  }

  @Override
  public double getEdgesInsideCommunity(int nodeCommunity, int communityNodes) {
    throw new NotImplementedException();
  }

  @Override
  public double getCommunityWeight(int community) {
    throw new NotImplementedException();
  }

  @Override
  public double getNodeCommunityWeight(int nodeCommunity) {
    throw new NotImplementedException();
  }

  @Override
  public void moveNode(int from, int to) {
    throw new NotImplementedException();
  }

  @Override
  public double getGraphWeightSum() {
    return 0;
  }

  @Override
  public int reInitializeCommunities() {
    Map<Integer, Integer> initCommunities = new HashMap<>();
    int communityCounter = 0;
    for (Object o : graph.getAll(NodeQueries.nodeType())) {
      Node n = (Node) o;
      if (!initCommunities.containsKey(n.getCommunity())) {
        initCommunities.put(n.getCommunity(), communityCounter);
        communityCounter++;
      }
      int newCommunity = initCommunities.get(n.getCommunity());
      n.setCommunity(newCommunity);
      n.setNodeCommunity(newCommunity);
      graph.update(n);
    }
    return communityCounter;
  }

  @Override
  public int getCommunityFromNode(int nodeId) {
    return ((Node) graph.getOne(NodeQueries.queryById(nodeId))).getCommunity();
  }

  @Override
  public int getCommunity(int nodeCommunity) {
    return ((Node) graph.getOne(NodeQueries.queryByCommunity(nodeCommunity))).getCommunity();
  }

  @Override
  public int getCommunitySize(int community) {
    return hg.getAll(graph, NodeQueries.queryByCommunity(community)).size();
  }

  @Override
  public Map<Integer, List<Integer>> mapCommunities(int numberOfCommunities) {
    throw new NotImplementedException();
  }

  @Override
  public boolean nodeExists(int nodeId) {
    return !hg.getAll(graph, NodeQueries.queryById(nodeId)).isEmpty();
  }

  private void createSchema() {
    RelTypeSimilar.addTo(graph);
  }

  private void createHyperGraphDB() {
    this.graph = HGEnvironment.get(dbStorageDirectory.getAbsolutePath(), getConfiguration());
  }

  private HGConfiguration getConfiguration() {
    HGConfiguration config = new HGConfiguration();
    config.setTransactional(false);
    config.setSkipOpenedEvent(true);

    return config;
  }
}

package eu.socialsensor.graphdatabases.hypergraph;


import com.google.common.collect.Iterables;
import eu.socialsensor.graphdatabases.GraphDatabaseBase;
import eu.socialsensor.graphdatabases.hypergraph.hyperedge.HEIsSimilar;
import eu.socialsensor.graphdatabases.hypergraph.vertex.Node;
import eu.socialsensor.graphdatabases.hypergraph.vertex.NodeQueries;
import eu.socialsensor.insert.HyperGraphDBDistributedSingleInsertion;
import eu.socialsensor.insert.HyperGraphDBMassiveInsertion;
import eu.socialsensor.insert.HyperGraphDBSingleInsertion;
import eu.socialsensor.insert.Insertion;
import eu.socialsensor.main.BenchmarkConfiguration;
import eu.socialsensor.main.GraphDatabaseType;
import eu.socialsensor.main.P2P;
import eu.socialsensor.utils.Utils;
import org.hypergraphdb.HGConfiguration;
import org.hypergraphdb.HGEnvironment;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.algorithms.DefaultALGenerator;
import org.hypergraphdb.algorithms.GraphClassics;
import org.hypergraphdb.algorithms.HGALGenerator;
import org.hypergraphdb.atom.HGRel;
import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.query.HGQueryCondition;
import org.hypergraphdb.storage.bje.BJEConfig;

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
                        Iterator<Node>,
                        Iterator<HGRel>,
                        Node,
                        HGRel
                        > {

    protected HyperGraph graph = null;
    protected HyperGraphPeer peer = null;

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

    @Override
    public Node getOtherVertexFromEdge(
            HGRel r,
            Node oneVertex
    ) {
        for (HGHandle hgHandle : r) {
            Node n = graph.get(hgHandle);
            if (!n.equals(oneVertex)) {
                return n;
            }
        }
        return null;
    }

    @Override
    public Node getSrcVertexFromEdge(
            HGRel edge
    ) {
        return graph.get(edge.getTargetAt(0));
    }

    @Override
    public Node getDestVertexFromEdge
            (
                    HGRel edge
            ) {
        return graph.get(edge.getTargetAt(1));
    }

    @Override
    public Node getVertex
            (
                    Integer nodeId
            ) {
        return graph.getOne(NodeQueries.queryById(nodeId));
    }

    @Override
    public Iterator<HGRel> getAllEdges() {
        HGHandle areSimilar = HEIsSimilar.getHGRelType(graph);
        return graph.<HGRel>getAll(hg.and(hg.type(areSimilar))).iterator();
    }

    @Override
    public Iterator<HGRel> getNeighborsOfVertex(
            Node v
    ) {
        return graph.<HGRel>getAll(
                HEIsSimilar.getAllInAndOutNeighbors(
                        graph,
                        graph.getHandle(v)
                )
        ).iterator();
    }

    @Override
    public boolean edgeIteratorHasNext(
            Iterator<HGRel> it
    ) {
        return it.hasNext();
    }

    @Override
    public HGRel nextEdge(
            Iterator<HGRel> it
    ) {
        return it.next();
    }

    @Override
    public void cleanupEdgeIterator(
            Iterator<HGRel> it
    ) {
    }

    @Override
    public Iterator<Node> getVertexIterator() {
        return graph.<Node>getAll(NodeQueries.nodeType()).iterator();
    }

    @Override
    public boolean vertexIteratorHasNext(
            Iterator<Node> it
    ) {
        return it.hasNext();
    }

    @Override
    public Node nextVertex(
            Iterator<Node> it
    ) {
        return it.next();
    }

    @Override
    public void cleanupVertexIterator(
            Iterator<Node> it
    ) {
    }

    @Override
    public void open() {
        createHyperGraphDB();
    }

    @Override
    public void createGraphForSingleLoad() {
        createHyperGraphDB();
        createSchema();
    }

    @Override
    public void createGraphForDistributedSingleLoad() {
        try {
            this.peer = P2P.getPeer();
            this.graph = peer.getGraph();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void massiveModeLoading(File dataPath) {
        Insertion insertion = new HyperGraphDBMassiveInsertion(graph);
        insertion.createGraph(dataPath, 0);
    }

    @Override
    public void singleModeLoading(File dataPath, File resultsPath, int scenarioNumber) {
        Insertion insertion = new HyperGraphDBSingleInsertion(graph, resultsPath);
        insertion.createGraph(dataPath, scenarioNumber);
    }

    @Override
    public void distributedSingleModeLoading(File dataPath, File resultsPath, int scenarioNumber) {
        Insertion insertion = new HyperGraphDBDistributedSingleInsertion(peer, resultsPath);
        insertion.createGraph(dataPath, scenarioNumber);

    }

    @Override
    public void createGraphForMassiveLoad() {
        createHyperGraphDB();
        createSchema();
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
        List<HGRel> neighbors = graph.getAll(
                HEIsSimilar.getAllOutgoingNeighbors(
                        graph,
                        graph.findOne(NodeQueries.queryById(nodeId)
                        )
                )
        );

        return neighbors
                .stream()
                .map(rel -> rel.getTargetAt(1))
                .map(graph::<Node>get)
                .map(Node::getId)
                .collect(Collectors.toSet());
    }

    @Override
    public double getNodeWeight(int nodeId) {
        HGHandle node = graph.getOne(NodeQueries.queryById(nodeId));
        HGQueryCondition inAndOutNeighbors = HEIsSimilar.getAllInAndOutNeighbors(graph, node);
        return graph.getAll(inAndOutNeighbors).size();
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
        Set<Integer> communities = new HashSet<>();
        List<HGHandle> handle = graph.findAll(NodeQueries.queryByCommunity(nodeCommunities));
        for (HGHandle h : handle) {
            List<HGRel> rels = graph.getAll(HEIsSimilar.getAllOutgoingNeighbors(graph, h));
            for (HGRel r : rels) {
                Node neighbour = graph.get(r.getTargetAt(1));
                communities.add(neighbour.getCommunity());
            }
        }
        return communities;
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
        double edges = 0;
        List<HGHandle> nodes = graph.findAll(NodeQueries.queryByNodeCommunity(nodeCommunity));
        List<HGHandle> comNodes = graph.findAll(NodeQueries.queryByCommunity(communityNodes));
        for (HGHandle node : nodes) {
            for (HGRel rel : graph.<HGRel>getAll(HEIsSimilar.getAllOutgoingNeighbors(graph, node))) {
                if (Iterables.contains(comNodes, rel.getTargetAt(1))) {
                    edges++;
                }
            }
        }
        return edges;
    }

    @Override
    public double getCommunityWeight(int community) {
        return getNodeCommunityWeight(community); // ToDo remove duplicated method from interface
    }

    @Override
    public double getNodeCommunityWeight(int nodeCommunity) {
        double nodeCommunityWeight = 0;
        List<HGHandle> iter = graph.findAll(NodeQueries.queryByCommunity(nodeCommunity));
        for (HGHandle n : iter) {
            HGQueryCondition outgoingNeighbors = HEIsSimilar.getAllOutgoingNeighbors(graph, n);
            nodeCommunityWeight += graph.findAll(outgoingNeighbors).size();
        }
        return nodeCommunityWeight;
    }

    @Override
    public void moveNode(int from, int to) {
        Iterable<HGHandle> fromIter = graph.findAll(NodeQueries.queryByNodeCommunity(from));
        for (HGHandle handle : fromIter) {
            Node n = graph.get(handle);
            n.setCommunity(to);
            graph.replace(handle, n);
        }
    }

    @Override
    public double getGraphWeightSum() {
        return Collections.singletonList(getAllEdges()).size();
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
        Map<Integer, List<Integer>> communities = new HashMap<>();
        for (int i = 0; i < numberOfCommunities; i++) {
            List<Node> nodesIter = graph.getAll(NodeQueries.queryByCommunity(i));
            List<Integer> nodes = new ArrayList<>();
            for (Node n : nodesIter) {
                nodes.add(n.getId());
            }
            communities.put(i, nodes);
        }
        return communities;
    }

    @Override
    public boolean nodeExists(int nodeId) {
        return !hg.getAll(graph, NodeQueries.queryById(nodeId)).isEmpty();
    }

    private void createSchema() {
        HEIsSimilar.addTo(graph);
    }

    private void createHyperGraphDB() {
        this.graph = HGEnvironment.get(dbStorageDirectory.getAbsolutePath(), getConfiguration());
    }

    private HGConfiguration getConfiguration() {
        HGConfiguration config = new HGConfiguration();
        config.setTransactional(false);
        config.setSkipOpenedEvent(true);
        BJEConfig storeConfig = (BJEConfig) config.getStoreImplementation().getConfiguration();
        storeConfig.getEnvironmentConfig().setCacheSize(1024 * 1024 * 1000);
        return config;
    }
}

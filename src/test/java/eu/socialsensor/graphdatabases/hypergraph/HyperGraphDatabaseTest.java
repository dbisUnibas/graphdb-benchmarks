package eu.socialsensor.graphdatabases.hypergraph;


import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import eu.socialsensor.graphdatabases.hypergraph.vertex.Node;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.hypergraphdb.HGEnvironment;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.atom.HGRel;
import org.junit.Assert;
import org.junit.Test;

public class HyperGraphDatabaseTest {

  private File databaseDir = new File(this.getClass().getResource("/").getPath(), "data");
  private File testData = new File(this.getClass().getResource("/test.data").getPath());
  private File resultDir = new File(this.getClass().getResource("/").getPath(), "result");

  private HyperGraphDatabase graph = null;

  private void cleaDBSetup(boolean isSingleMode) throws IOException {
    FileUtils.deleteDirectory(databaseDir);
    FileUtils.deleteDirectory(resultDir);

    FileUtils.forceMkdir(databaseDir);
    FileUtils.forceMkdir(resultDir);

    graph = new HyperGraphDatabase(null, databaseDir);
    graph.open();
    if (isSingleMode) {
      graph.createGraphForSingleLoad();
      graph.singleModeLoading(testData, resultDir, 0);
    } else {
      graph.massiveModeLoading(testData);
    }
  }

  @Test
  public void createDatabaseForSingleMode() throws IOException {
    this.cleaDBSetup(true);
    Assert.assertTrue("Should create a database file",
        new File(databaseDir, "00000000.jdb").exists()
    );
    Assert.assertEquals("Should match node count",
        graph.getNodeCount(), 5);

    Iterator<HGRel> it = graph.getAllEdges();
    HyperGraph graph = HGEnvironment.get(databaseDir.getAbsolutePath());
    List<String> resultRelationships = new ArrayList<>();
    while (it.hasNext()) {
      HGRel t = it.next();
      Node n0 = graph.get(t.getTargetAt(0));
      Node n1 = graph.get(t.getTargetAt(1));
      resultRelationships.add(n0.getId() + " -> " + n1.getId());
    }
    graph.close();

    Assert.assertEquals("Should contain same relationships",
        CollectionUtils.getCardinalityMap(resultRelationships),
        CollectionUtils.getCardinalityMap(getExpectedRelationships()));
  }

  private List<String> getExpectedRelationships() {
    List<String> expectedRelationships = new ArrayList<>();
    expectedRelationships.add("0 -> 1");
    expectedRelationships.add("1 -> 2");
    expectedRelationships.add("2 -> 3");
    expectedRelationships.add("3 -> 4");
    expectedRelationships.add("1 -> 4");
    return expectedRelationships;
  }

  private List<Integer> getExpectedNodeIds(){
    return  Arrays.asList(0, 1, 2, 3, 4);
  }

  @Test
  public void createDatabaseForMassiveMode() throws IOException {
    this.cleaDBSetup(false);
    assertTrue(new File(databaseDir, "00000000.jdb").exists());
    assertEquals(graph.getNodeCount(), 4);
  }

  @Test
  public void testSingleModeLoading() throws IOException {
    this.cleaDBSetup(true);
  }

  @Test
  public void initCommunityProperty() throws IOException {
    this.cleaDBSetup(true);
    graph.initCommunityProperty();
  }

  @Test
  public void getOtherVertexFromEdge() {
  }

  @Test
  public void getSrcVertexFromEdge() {
  }

  @Test
  public void getDestVertexFromEdge() {
  }

  @Test
  public void getVertex() {
  }

  @Test
  public void getAllEdges() {
  }

  @Test
  public void getNeighborsOfVertex() {
  }

  @Test
  public void edgeIteratorHasNext() {
  }

  @Test
  public void nextEdge() {
  }

  @Test
  public void cleanupEdgeIterator() {
  }

  @Test
  public void getVertexIterator() throws IOException {
    cleaDBSetup(true);
    List<Integer> nodeId = new ArrayList<>();
    graph.getVertexIterator().forEach(n -> nodeId.add(n.getId()));

    Assert.assertEquals("Should retrieve all node ids",
        5,
        nodeId.size()
    );

    Assert.assertEquals("Should match node ids",
        CollectionUtils.getCardinalityMap(getExpectedNodeIds()),
        CollectionUtils.getCardinalityMap(nodeId));
  }

  @Test
  public void vertexIteratorHasNext() {
  }

  @Test
  public void nextVertex() {
  }

  @Test
  public void cleanupVertexIterator() {
  }

  @Test
  public void open() {
  }

  @Test
  public void createGraphForSingleLoad() {
  }

  @Test
  public void massiveModeLoading() {
  }

  @Test
  public void singleModeLoading() {
  }

  @Test
  public void createGraphForMassiveLoad() {
  }

  @Test
  public void shutdown() {
  }

  @Test
  public void delete() {
  }

  @Test
  public void shutdownMassiveGraph() {
  }

  @Test
  public void shortestPath() {
  }

  @Test
  public void getNodeCount() {
  }

  @Test
  public void getNeighborsIds() {
  }

  @Test
  public void getNodeWeight() {
  }

  @Test
  public void getCommunitiesConnectedToNodeCommunities() {
  }

  @Test
  public void getNodesFromCommunity() {
  }

  @Test
  public void getNodesFromNodeCommunity() {
  }

  @Test
  public void getEdgesInsideCommunity() {
  }

  @Test
  public void getCommunityWeight() {
  }

  @Test
  public void getNodeCommunityWeight() {
  }

  @Test
  public void moveNode() {
  }

  @Test
  public void getGraphWeightSum() {
  }

  @Test
  public void reInitializeCommunities() {
  }

  @Test
  public void getCommunityFromNode() {
  }

  @Test
  public void getCommunity() {
  }

  @Test
  public void getCommunitySize() {
  }

  @Test
  public void mapCommunities() {
  }

  @Test
  public void nodeExists() {
  }
}
package eu.socialsensor.insert;


import eu.socialsensor.graphdatabases.hypergraph.edge.RelTypeSimilar;
import eu.socialsensor.graphdatabases.hypergraph.vertex.Node;
import eu.socialsensor.graphdatabases.hypergraph.vertex.NodeQueries;
import eu.socialsensor.main.GraphDatabaseType;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.atom.HGRel;

import java.util.*;


/**
 * Implementation of massive Insertion in Neo4j graph database
 *
 * @author sotbeis, sotbeis@iti.gr
 * @author Alexander Patrikalakis
 */
public final class HyperGraphMassiveInsertion extends InsertionBase<Node> {

  private final HyperGraph hyperGraph;
  private final List<Node> nodeBatch = new ArrayList<>();
  private final List<List<Node>> relationships = new ArrayList<>();
  private final Map<Node, HGHandle> cache = new HashMap<>();


  @Override
  protected void post() {
    batchImportNodes();
    batchImportRelationships();
    hyperGraph.runMaintenance();
  }

  public HyperGraphMassiveInsertion(
      HyperGraph hyperGraph
  ) {
    super(GraphDatabaseType.HYPERGRAPH_DB, null);
    this.hyperGraph = hyperGraph;
    NodeQueries.addIndex(hyperGraph);
  }

  @Override
  protected Node getOrCreate(
      String nodeId
  ) {
    Node n = new Node();
    n.setId(Integer.parseInt(nodeId));
    this.nodeBatch.add(n);
    return n;
  }

  @Override
  protected void relateNodes(
      Node src,
      Node dest
  ) {
    relationships.add(Arrays.asList(src, dest));
  }

  private void batchImportNodes() {
    if (nodeBatch.isEmpty()) {
      return;
    }
    final Node first = nodeBatch.iterator().next();
    HGHandle handle = hyperGraph.add(first);
    cache.put(first, handle);

    final HGHandle typeHandle = hyperGraph.getType(handle);

    for (Node node : nodeBatch) {
      if (cache.get(node) == null) {
        cache.put(node, hyperGraph.add(node, typeHandle));
      }
    }
  }

  private void batchImportRelationships() {
    List<Node> firstRel = relationships.iterator().next();

    HGHandle src  = this.cache.get(firstRel.get(0));
    HGHandle dest = this.cache.get(firstRel.get(1));

    hyperGraph.add(
        new HGRel(src, dest),
        RelTypeSimilar.getHGRelType(hyperGraph)
    );
  }


}

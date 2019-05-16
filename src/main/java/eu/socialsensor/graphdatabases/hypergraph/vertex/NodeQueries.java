package eu.socialsensor.graphdatabases.hypergraph.vertex;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.indexing.ByPartIndexer;
import org.hypergraphdb.query.AtomTypeCondition;
import org.hypergraphdb.query.HGQueryCondition;

public class NodeQueries {

  private static AtomTypeCondition atom = hg.type(Node.class);

  public static HGQueryCondition nodeType() {
    return hg.and(atom);
  }

  public static HGQueryCondition queryById(int id) {
    return hg.and(atom, hg.eq("id", id));
  }

  public static HGQueryCondition queryByCommunity(int community) {
    return hg.and(atom, hg.eq("community", community));
  }

  public static HGQueryCondition queryByNodeCommunity(int nodeCommunity) {
    return hg.and(atom, hg.eq("community", nodeCommunity));
  }

  public static HGHandle getNodeTypeHandle(HyperGraph graph) {
    return graph.getTypeSystem().getTypeHandle(Node.class);
  }

  public static void addIndex(HyperGraph hyperGraph) {
    HGHandle bTypeH = hyperGraph.getTypeSystem().getTypeHandle(Node.class);
    hyperGraph.getIndexManager().register(new ByPartIndexer(bTypeH, "id"));
    hyperGraph.getIndexManager().register(new ByPartIndexer(bTypeH, "community"));
    hyperGraph.getIndexManager().register(new ByPartIndexer(bTypeH, "communityNode"));
  }
}

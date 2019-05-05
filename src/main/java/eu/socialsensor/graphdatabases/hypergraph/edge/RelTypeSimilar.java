package eu.socialsensor.graphdatabases.hypergraph.edge;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.atom.HGRelType;

public class RelTypeSimilar {

  private static final String SIMILAR = "similar";
  private static final HGRelType similarRelType = new HGRelType(SIMILAR);

  public static HGHandle getHGRelType(HyperGraph graph) {
    return graph.findOne(
        hg.and(
            hg.type(HGRelType.class),
            hg.eq("name", SIMILAR)
        )
    );
  }

  public static void addTo(HyperGraph graph) {
    graph.add(similarRelType);
  }
}

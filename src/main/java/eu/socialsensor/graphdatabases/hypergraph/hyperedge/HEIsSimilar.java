package eu.socialsensor.graphdatabases.hypergraph.hyperedge;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.atom.HGRelType;
import org.hypergraphdb.query.HGQueryCondition;

/**
 * Hyper Graph
 * Hyperedge: is similar
 *
 * @author Fabrizio Parrillo
 */
public class HEIsSimilar {

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

    public static HGQueryCondition getAllInAndOutNeighbors(HyperGraph graph, HGHandle atom) {
        return hg.and(
                hg.type(getHGRelType(graph)),
                hg.incident(atom)
        );
    }

    public static HGQueryCondition getAllOutgoingNeighbors(HyperGraph graph, HGHandle atom) {
        return hg.and(
                hg.type(getHGRelType(graph)),
                hg.incidentAt(atom, 0)
        );

    }

    public static HGQueryCondition getAllIngoingNeighbors(HyperGraph graph, HGHandle atom) {
        return hg.and(
                hg.type(getHGRelType(graph)),
                hg.incidentAt(atom, 1)
        );
    }

    public static void addTo(HyperGraph graph) {
        graph.add(similarRelType);
    }
}

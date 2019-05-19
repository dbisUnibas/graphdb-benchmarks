package eu.socialsensor.insert;


import eu.socialsensor.graphdatabases.hypergraph.hyperedge.HEIsSimilar;
import eu.socialsensor.graphdatabases.hypergraph.vertex.Node;
import eu.socialsensor.graphdatabases.hypergraph.vertex.NodeQueries;
import eu.socialsensor.main.GraphDatabaseType;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGQuery;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.atom.HGRel;

import java.io.File;


/**
 * Implementation of single insertion in HyperGraph graph database
 *
 * @author fuubi, fabriparrillo@hotmail.com
 * @author Fabrizio Parrillo
 */
@SuppressWarnings("deprecation")
public class HyperGraphDBSingleInsertion extends InsertionBase<HGHandle> {
    private final HyperGraph hyperGraph;
    private final HGHandle nodeHandleType;
    public HyperGraphDBSingleInsertion(HyperGraph hyperGraph, File resultsPath ) {
        super( GraphDatabaseType.HYPERGRAPH_DB, resultsPath );
        this.hyperGraph = hyperGraph;
        this.nodeHandleType = NodeQueries.getNodeTypeHandle(hyperGraph);
    }

    public HGHandle getOrCreate( String nodeId ) {
        Node n = new Node(Integer.parseInt(nodeId), 0,0);
        HGHandle handle = HGQuery.hg.assertAtom(hyperGraph, n, nodeHandleType);
        NodeQueries.addIndex(hyperGraph);
        hyperGraph.runMaintenance();
        return handle;
    }

    @Override
    public void relateNodes( HGHandle src, HGHandle dest ) {
        hyperGraph.add(
                new HGRel(src, dest),
                HEIsSimilar.getHGRelType(hyperGraph)
        );
    }

}

package eu.socialsensor.insert;


import eu.socialsensor.graphdatabases.hypergraph.edge.RelTypeSimilar;
import eu.socialsensor.graphdatabases.hypergraph.vertex.Node;
import eu.socialsensor.graphdatabases.hypergraph.vertex.NodeQueries;
import eu.socialsensor.main.GraphDatabaseType;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGQuery;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.atom.HGRel;
import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.peer.cact.ReplaceAtom;

import java.io.File;


/**
 * Implementation of a distributed single Insertion in Hypergraph graph database
 * This node acts as a master which acts as a coordinator.
 *
 * @author fuubi, fabriparrillo@hotmail.com
 * @author Fabrizio Parrillo
 */
@SuppressWarnings("deprecation")
public class HyperGraphDbDistributedSingleInsertion extends InsertionBase<HGHandle> {
    private final HGHandle nodeHandleType;
    private final HGHandle relTypeSimilar;

    private final HyperGraphPeer coordinator;
    private final HyperGraph hyperGraph;

    public HyperGraphDbDistributedSingleInsertion(HyperGraphPeer coordinator, File resultsPath ) {
        super( GraphDatabaseType.HYPERGRAPH_DB, resultsPath );
        this.coordinator = coordinator;
        this.hyperGraph = coordinator.getGraph();
        this.nodeHandleType = NodeQueries.getNodeTypeHandle(hyperGraph);
        this.relTypeSimilar =  RelTypeSimilar.getHGRelType(hyperGraph);
        NodeQueries.addIndex(hyperGraph);
    }

    public HGHandle getOrCreate( String nodeId ) {
        Node n = new Node(Integer.parseInt(nodeId), 0,0);
        HGHandle handle = HGQuery.hg.assertAtom(coordinator.getGraph(), n, nodeHandleType);
        this.replicate(handle, n);
        hyperGraph.runMaintenance(); // indexing
        return handle;
    }

    @Override
    public void relateNodes( HGHandle src, HGHandle dest ) {
        HGRel rel = new HGRel(
                src,
                dest
        );
        HGHandle handle = hyperGraph.add(
                rel,
                RelTypeSimilar.getHGRelType(hyperGraph)
        );
        this.replicate(handle, rel);
    }

    private void replicate(HGHandle nodeHandle, Node node ){
        this.coordinator.getConnectedPeers().stream()
                .map(id -> new ReplaceAtom(coordinator, nodeHandle, node, this.nodeHandleType,  id))
                .map(replaceActivity ->
                        this.coordinator
                                .getActivityManager()
                                .initiateActivity(replaceActivity)
                );
    }

    private void replicate(HGHandle relHandle, HGRel rel ){
        this.coordinator.getConnectedPeers().stream()
                .map(id -> new ReplaceAtom(coordinator, relHandle, rel, this.nodeHandleType,  id))
                .map(replaceActivity ->
                        this.coordinator
                                .getActivityManager()
                                .initiateActivity(replaceActivity)
                );

    }
}

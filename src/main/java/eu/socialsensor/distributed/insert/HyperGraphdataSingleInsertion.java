package eu.socialsensor.distributed.insert;


import eu.socialsensor.graphdatabases.hypergraph.edge.RelTypeSimilar;
import eu.socialsensor.graphdatabases.hypergraph.vertex.Node;
import eu.socialsensor.graphdatabases.hypergraph.vertex.NodeQueries;
import eu.socialsensor.insert.InsertionBase;
import eu.socialsensor.main.GraphDatabaseType;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.peer.HGPeerIdentity;
import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.peer.cact.AddAtom;
import org.hypergraphdb.peer.workflow.ActivityResult;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;


/**
 * Implementation of a distributed single Insertion in Hypergraph graph database
 * This node acts as a master which acts as a coordinator.
 *
 * @author fuubi, fabriparrillo@hotmail.com
 * @author Fabrizio Parrillo
 */
@SuppressWarnings("deprecation")
public class HyperGraphdataSingleInsertion extends InsertionBase<HGHandle> {
    private final HGHandle nodeHandleType;
    private final HGHandle relTypeSimilar;

    private final HyperGraphPeer coordinator;
    private final Map<Integer, HGPeerIdentity> peers;

    public HyperGraphdataSingleInsertion(HyperGraphPeer coordinator, File resultsPath ) {
        super( GraphDatabaseType.HYPERGRAPH_DB, resultsPath );
        this.coordinator = coordinator;
        this.nodeHandleType = NodeQueries.getNodeTypeHandle(coordinator.getGraph());
        this.relTypeSimilar =  RelTypeSimilar.getHGRelType(coordinator.getGraph());

        this.peers = new HashMap<>();
        this.peers.put(0, coordinator.getIdentity());
        for (HGPeerIdentity connectedPeer : coordinator.getConnectedPeers()) {
            int id = peers.size() + 1;
            this.peers.put(id, connectedPeer);
        }
    }

    public HGHandle getOrCreate( String nodeId ) {
       Node n = new Node(Integer.parseInt(nodeId), 0,0);

       int peerId = Integer.parseInt(nodeId) % this.peers.size();
       Future<ActivityResult> result = coordinator.getActivityManager().initiateActivity(
            new AddAtom(coordinator, n, peers.get(peerId))
       );

        AddAtom addAtomResult = null;
        try {
            addAtomResult = (AddAtom) result.get().getActivity();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        index(); // Todo: add workflow
        return addAtomResult.getAtomHandle();
    }

    @Override
    public void relateNodes( HGHandle src, HGHandle dest ) {
/*
        Future<ActivityResult> result = coordinator.getActivityManager().initiateActivity(
                new GetAtom(coordinator, n, peers.get(peerId))
        );
        hyperGraph.add(
                new HGRel(src, dest),

        );*/
    }

    public void index(){
/*        HGHandle bTypeH = hyperGraph.getTypeSystem().getTypeHandle(Node.class);
        hyperGraph.getIndexManager().register(new ByPartIndexer(bTypeH, "id"));
        hyperGraph.getIndexManager().register(new ByPartIndexer(bTypeH, "community"));
        hyperGraph.getIndexManager().register(new ByPartIndexer(bTypeH, "communityNode"));
        hyperGraph.runMaintenance();*/
    }
}
